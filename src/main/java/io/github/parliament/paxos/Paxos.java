package io.github.parliament.paxos;

import com.google.common.base.Strings;
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
import io.github.parliament.paxos.client.InetLearner;
import io.github.parliament.paxos.client.PeerAcceptors;
import io.github.parliament.paxos.proposer.Proposer;
import lombok.Builder;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

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
            .build(new CacheLoader<Integer, CompletableFuture<byte[]>>() {
                @Override
                public CompletableFuture<byte[]> load(Integer key) {
                    return new CompletableFuture<>();
                }
            });
    private volatile ExecutorService executorService;
    private Sequence<String> sequence;
    private Persistence persistence;
    private PeerAcceptors peerAcceptors;
    private InetLearner learner;
    private volatile int max = -1;
    private volatile int min = -1;
    private volatile int done = -1;

    class LocalAcceptorWithPersistence extends LocalAcceptor {
        LocalAcceptorWithPersistence(int round) {
            super(round);
        }

        LocalAcceptorWithPersistence(int round, String np, String na, byte[] va) {
            super(round);
            setNp(np);
            setNa(na);
            setVa(va);
        }

        @Override
        public void decide(byte[] agreement) throws Exception {
            persistence.put((round + "agreement").getBytes(), agreement);
            persistence();
            if (round > max) {
                max(round);
            }
            proposals.get(round).complete(agreement);
        }

        @Override
        public void persistence() throws IOException, ExecutionException {
            persistenceAcceptor(round, this);
        }
    }

    @Builder
    private Paxos(@NonNull ExecutorService executorService,
                  @NonNull Sequence<String> sequence,
                  @NonNull Persistence persistence,
                  @NonNull PeerAcceptors peerAcceptors,
                  @NonNull InetLearner learner) throws IOException, ExecutionException {
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
    public void coordinate(int round, byte[] content) throws ExecutionException, IOException {
        byte[] agreement = get(round);
        if (agreement != null) {
            proposals.get(round).complete(agreement);
            return;
        }
        Acceptor me = create(round);
        List<? extends Acceptor> others = peers(round);
        List<Acceptor> peers = new ArrayList<>(others);
        peers.add(me);
        Proposer proposer = proposers.computeIfAbsent(round, (r) -> new Proposer(peers, sequence, content));

        executorService.submit(() -> proposer.propose((result) -> {
            peerAcceptors.release(round);
        }));
    }

    @Override
    public Future<byte[]> instance(int round) throws ExecutionException, IOException {
        byte[] r = get(round);
        if (r != null) {
            proposals.get(round).complete(r);
        }
        return proposals.get(round);
    }

    @Override
    public int min() {
        return min;
    }

    private void min(int m) throws IOException, ExecutionException {
        this.min = m;
        persistence.put("min".getBytes(), ByteBuffer.allocate(4).putInt(m).array());
    }

    @Override
    public void learn(int round) throws IOException, ExecutionException {
        byte[] content = learner.learn(round).orElse(null);
        if (content == null) {
            return;
        }
        persistence.put((round + "agreement").getBytes(), content);
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
                logger.warn("forgot others not finished states.It's impossible.A Bug?");
                return;
            }
            int min1 = cursor;
            if (cursor < 0) {
                return;
            }
            int m = Math.max(0, min());
            do {
                deleteAcceptor(cursor);
                persistence.del((cursor + "agreement").getBytes());
                cursor--;
            } while (cursor >= 0 && cursor > m);
            min(min1);
        }
    }

    @Override
    public byte[] get(int round) throws IOException, ExecutionException {
        return persistence.get((round + "agreement").getBytes());
    }

    private List<? extends Acceptor> peers(int round) {
        return peerAcceptors.create(round);
    }

    @Override
    public Acceptor create(int round) throws ExecutionException {
        return acceptors.get(round, () -> {
            Optional<LocalAcceptor> optAcceptor = regainAcceptor(round);
            return optAcceptor.orElse(new LocalAcceptorWithPersistence(round));
        });
    }

    void persistenceAcceptor(int round, LocalAcceptor acceptor) throws IOException, ExecutionException {
        if (Strings.isNullOrEmpty(acceptor.getNp())) {
            return;
        }
        persistence.put((round + "np").getBytes(), acceptor.getNp().getBytes());
        if (!Strings.isNullOrEmpty(acceptor.getNa())) {
            persistence.put((round + "na").getBytes(), acceptor.getNa().getBytes());
        }
        if (acceptor.getVa() != null) {
            persistence.put((round + "va").getBytes(), acceptor.getVa());
        }
    }

    Optional<LocalAcceptor> regainAcceptor(int round) throws IOException, ExecutionException {
        byte[] np = persistence.get((round + "np").getBytes());

        if (np == null) {
            return Optional.empty();
        }
        byte[] na = persistence.get((round + "na").getBytes());
        byte[] va = persistence.get((round + "va").getBytes());

        String nps = new String(np);
        String nas = na == null ? null : new String(na);

        return Optional.of(new LocalAcceptorWithPersistence(round, nps, nas, va));
    }

    void deleteAcceptor(int round) throws IOException, ExecutionException {
        persistence.del((round + "np").getBytes());
        persistence.del((round + "na").getBytes());
        persistence.del((round + "va").getBytes());
        persistence.del((round + "checksum").getBytes());
    }
}
