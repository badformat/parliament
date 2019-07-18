package io.github.parliament.paxos;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.MapMaker;
import io.github.parliament.Coordinator;
import io.github.parliament.Persistence;
import io.github.parliament.Sequence;
import io.github.parliament.paxos.acceptor.*;
import io.github.parliament.paxos.client.InetLeaner;
import io.github.parliament.paxos.client.PeerAcceptors;
import io.github.parliament.paxos.proposer.Proposer;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

//TODO acceptor need persistence
public class Paxos implements Coordinator, LocalAcceptors {
    private static final Logger logger = LoggerFactory.getLogger(Paxos.class);
    private final ConcurrentMap<Integer, Proposer> proposers = new MapMaker()
            .weakValues()
            .makeMap();
    private final Cache<Integer, LocalAcceptor> acceptors = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.of(10, ChronoUnit.MINUTES))
            .build();
    private final LoadingCache<Integer, CompletableFuture<byte[]>> proposals = CacheBuilder.newBuilder()
            .weakValues()
            .build(new CacheLoader<>() {
                @Override
                public CompletableFuture<byte[]> load(Integer key) {
                    return new CompletableFuture<>();
                }
            });
    private volatile ExecutorService executorService;
    private Sequence<String> sequence;
    private Persistence persistence;
    private PeerAcceptors peerAcceptors;
    private InetLeaner learner;
    private volatile int max = -1;
    private volatile int min = -1;
    private volatile int done = -1;

    @Builder
    private Paxos(@NonNull ExecutorService executorService,
                  @NonNull Sequence<String> sequence,
                  @NonNull Persistence persistence,
                  @NonNull PeerAcceptors peerAcceptors,
                  @NonNull InetLeaner learner) throws IOException, ExecutionException {
        this.executorService = executorService;
        this.sequence = sequence;
        this.persistence = persistence;
        this.peerAcceptors = peerAcceptors;
        this.learner = learner;

        byte[] bytes = persistence.get("min".getBytes());
        if (bytes != null) {
            min = ByteBuffer.wrap(bytes).getInt();
        }

        bytes = persistence.get("max".getBytes());
        if (bytes != null) {
            max = ByteBuffer.wrap(bytes).getInt();
        }

        bytes = persistence.get("done".getBytes());
        if (bytes != null) {
            done = ByteBuffer.wrap(bytes).getInt();
        }
    }

    @Override
    public void coordinate(int round, byte[] content) throws ExecutionException {
        byte[] agreement = get(round);
        if (agreement != null) {
            proposals.get(round).complete(agreement);
            return;
        }
        List<Acceptor> peers = new ArrayList<>();
        Acceptor me = create(round);
        List<? extends Acceptor> others = peers(round);
        peers.addAll(others);
        peers.add(me);
        Proposer proposer = proposers.computeIfAbsent(round, (r) -> new Proposer(peers, sequence, content));

        executorService.submit(() -> proposer.propose((result) -> {
            peerAcceptors.release(round);
        }));
    }

    @Override
    public Future<byte[]> instance(int round) throws ExecutionException {
        byte[] r = get(round);
        if (r != null) {
            proposals.get(round).complete(r);
        }
        return proposals.get(round);
    }

    @Override
    public void instance(int round, byte[] content) throws IOException, ExecutionException {
        byte[] r = get(round);
        if (r != null) {
            Preconditions.checkState(Arrays.equals(r, content));
        }
        put(round, content);
    }

    @Override
    public int min() {
        return min;
    }

    void min(int m) throws IOException, ExecutionException {
        this.min = m;
        persistence.put("min".getBytes(), ByteBuffer.allocate(4).putInt(m).array());
    }

    @Override
    public void learn(int round) throws IOException, ExecutionException {
        byte[] content = learner.instance(round).orElse(null);
        if (content == null) {
            return;
        }
        checkAndPut(round, content);
    }

    @Override
    public int done() {
        return done;
    }

    @Override
    public void done(int d) throws IOException, ExecutionException {
        this.done = d;
        persistence.put("done".getBytes(), ByteBuffer.allocate(4).putInt(d).array());
    }

    @Override
    public int max() {
        return max;
    }

    @Override
    public void max(int m) throws IOException, ExecutionException {
        max = m;
        persistence.put("max".getBytes(), ByteBuffer.allocate(4).putInt(max).array());
    }

    @Override
    public void forget(int before) throws IOException, ExecutionException {
        synchronized (this) {
            if (before > done) {
                return;
            }
            int other = learner.done();
            int cursor = Math.min(before, other);
            if (cursor < min) {
                logger.warn("forgotten others not finished states.It's impossible.A Bug?");
                return;
            }
            int min1 = cursor;
            if (cursor < 0) {
                return;
            }
            int m = Math.max(0, min());
            do {
                persistence.del(ByteBuffer.allocate(4).putInt(cursor).array());
                cursor--;
            } while (cursor >= 0 && cursor > m);
            min(min1);
        }
    }

    List<? extends Acceptor> peers(int round) {
        return peerAcceptors.create(round);
    }

    @Builder
    private static class FinishedAcceptor implements Acceptor {
        @NonNull
        @Getter(AccessLevel.PRIVATE)
        private byte[] finished;

        @Override
        public Prepare prepare(String n) {
            return Prepare.reject(n);
        }

        @Override
        public Accept accept(String n, byte[] value) {
            return Accept.reject(n);
        }

        @Override
        public void decide(byte[] agreement) {
            if (!Arrays.equals(finished, agreement)) {
                logger.error("Instance is already decided, but later consensus is different.A bug?");
                throw new IllegalStateException("Instance is already decided, but later consensus is different");
            }
        }
    }

    @Override
    public Acceptor create(int round) throws ExecutionException {
        byte[] consensus = get(round);
        if (consensus != null) {
            proposals.get(round).complete(consensus);
            return FinishedAcceptor.builder()
                    .finished(consensus).build();
        }

        return acceptors.get(round, () -> new LocalAcceptor(round) {
            @Override
            public void decide(byte[] agreement) throws Exception {
                checkAndPut(round, agreement);
                if (round > max) {
                    max(round);
                }
                proposals.get(round).complete(agreement);
            }
        });
    }

    private void checkAndPut(int round, byte[] agreement) throws IOException, ExecutionException {
        Preconditions.checkNotNull(agreement);
        byte[] r = get(round);
        if (r != null) {
            Preconditions.checkState(Arrays.equals(r, agreement), "decided value is not equals.");
        } else {
            put(round, agreement);
        }
    }

    public byte[] get(int round) {
        try {
            return persistence.get(ByteBuffer.allocate(4).putInt(round).array());
        } catch (IOException | ExecutionException e) {
            logger.warn("io exception.", e);
            return null;
        }
    }

    void put(int round, byte[] agreement) throws IOException, ExecutionException {
        persistence.put(ByteBuffer.allocate(4).putInt(round).array(), agreement);
    }
}
