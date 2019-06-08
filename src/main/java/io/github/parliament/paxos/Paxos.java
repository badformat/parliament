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
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptors;
import io.github.parliament.paxos.client.PeerAcceptors;
import io.github.parliament.paxos.proposer.Proposer;
import lombok.Builder;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class Paxos implements Coordinator, LocalAcceptors {
    private static final Logger logger = LoggerFactory.getLogger(Paxos.class);
    private final ConcurrentMap<Integer, Proposer> proposers = new MapMaker()
            .weakValues()
            .makeMap();
    private final Cache<Integer, LocalAcceptor> acceptors = CacheBuilder.newBuilder()
            .expireAfterWrite(120, TimeUnit.SECONDS)
            .build();
    private final LoadingCache<Integer, CompletableFuture<byte[]>> proposals = CacheBuilder.newBuilder()
            .expireAfterWrite(120, TimeUnit.SECONDS)
            .build(new CacheLoader<>() {
                @Override
                public CompletableFuture<byte[]> load(Integer key) throws Exception {
                    return new CompletableFuture<>();
                }
            });
    private volatile ExecutorService executorService;
    private Sequence<String> sequence;
    private Persistence persistence;
    private PeerAcceptors peerAcceptors;
    private volatile int max = -1;
    private volatile int min = -1;
    private volatile int done = -1;

    @Builder
    private Paxos(@NonNull ExecutorService executorService,
                  @NonNull Sequence<String> sequence,
                  @NonNull Persistence persistence,
                  @NonNull PeerAcceptors peerAcceptors) throws IOException {
        this.executorService = executorService;
        this.sequence = sequence;
        this.persistence = persistence;
        this.peerAcceptors = peerAcceptors;

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
        List<Acceptor> peers = new ArrayList<>();
        Acceptor me = create(round);
        List<? extends Acceptor> others = peers(round);
        peers.addAll(others);
        peers.add(me);
        Proposer proposer = proposers.computeIfAbsent(round, (r) -> new Proposer(peers, sequence, content));
        proposals.get(round).completeAsync(() -> proposer.propose((result) -> {
            peerAcceptors.release(round);
        }), executorService);
    }

    @Override
    public Future<byte[]> instance(int round) throws ExecutionException {
        byte[] r = get(round);
        if (r != null) {
            proposals.get(round).complete(get(round));
        }
        return proposals.get(round);
    }

    @Override
    public void instance(int round, byte[] content) throws IOException {
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

    void min(int m) throws IOException {
        this.min = m;
        persistence.put("min".getBytes(), ByteBuffer.allocate(4).putInt(m).array());
    }

    @Override
    public int done() {
        return done;
    }

    @Override
    public void done(int d) throws IOException {
        this.done = d;
        persistence.put("done".getBytes(), ByteBuffer.allocate(4).putInt(d).array());
    }

    @Override
    public int max() {
        return max;
    }

    @Override
    public void max(int m) throws IOException {
        max = m;
        persistence.put("max".getBytes(), ByteBuffer.allocate(4).putInt(max).array());
    }

    @Override
    public void forget(int before) throws IOException {
        Preconditions.checkState(before <= done);
        int other = peerAcceptors.done();
        int cursor = Math.min(before, other);
        if (cursor < 0) {
            return;
        }
        min(cursor + 1);
        boolean existed;
        do {
            existed = persistence.remove(ByteBuffer.allocate(4).putInt(cursor).array());
            cursor--;
        } while (existed && cursor >= 0);
    }

    List<? extends Acceptor> peers(int round) {
        return peerAcceptors.create(round);
    }

    @Override
    public Acceptor create(int round) throws ExecutionException {
        return acceptors.get(round, () -> new LocalAcceptor(round) {
            @Override
            public void decide(byte[] agreement) throws Exception {
                if (round > max) {
                    max(round);
                }
                CompletableFuture<byte[]> future = proposals.get(round);
                if (future != null && !future.isDone()) {
                    future.complete(agreement);
                }
                byte[] r = get(round);
                if (r != null) {
                    Preconditions.checkState(Arrays.equals(r, agreement), "decided value is not equals.");
                } else {
                    put(round, agreement);
                }
            }
        });
    }

    public byte[] get(int round) {
        try {
            return persistence.get(ByteBuffer.allocate(4).putInt(round).array());
        } catch (IOException e) {
            logger.warn("io exception.", e);
            return null;
        }
    }

    void put(int round, byte[] agreement) throws IOException {
        persistence.put(ByteBuffer.allocate(4).putInt(round).array(), agreement);
    }
}
