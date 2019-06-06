package io.github.parliament.paxos;

import com.google.common.base.Preconditions;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Paxos implements Coordinator, LocalAcceptors {
    private static final Logger logger = LoggerFactory.getLogger(Paxos.class);
    private final ConcurrentMap<Integer, Proposer> proposers = new MapMaker()
            .weakValues()
            .makeMap();
    private final ConcurrentMap<Integer, CompletableFuture<byte[]>> proposals = new MapMaker()
            .weakValues()
            .makeMap();
    private volatile ExecutorService executorService;
    private Sequence<String> sequence;
    private Persistence persistence;
    private PeerAcceptors peerAcceptors;
    private volatile int max = -1;

    @Builder
    private Paxos(@NonNull ExecutorService executorService,
                  @NonNull Sequence<String> sequence,
                  @NonNull Persistence persistence,
                  @NonNull PeerAcceptors peerAcceptors) {
        this.executorService = executorService;
        this.sequence = sequence;
        this.persistence = persistence;
        this.peerAcceptors = peerAcceptors;
    }

    @Override
    public void coordinate(int round, byte[] content) {
        Proposer proposer = proposers.computeIfAbsent(round, (r) -> new Proposer(create(r), peers(r), sequence, content));
        proposals.computeIfAbsent(round, (k) -> new CompletableFuture());
        proposals.get(round).completeAsync(proposer::propose, executorService);
    }

    @Override
    public Future<byte[]> instance(int round) {
        proposals.computeIfAbsent(round, (k) -> new CompletableFuture<>());
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
        return 0;
    }

    @Override
    public int max() {
        return max;
    }

    @Override
    public int max(int m) {
        return max = m;
    }

    @Override
    public void forget(int before) {

    }

    List<? extends Acceptor> peers(int round) {
        return peerAcceptors.create(round);
    }

    @Override
    public LocalAcceptor create(int r) {
        return new LocalAcceptor(r) {
            @Override
            public void decide(byte[] agreement) throws IOException {
                max = Math.max(round, max);
                peerAcceptors.release(round);
                CompletableFuture<byte[]> future = proposals.get(round);
                if (future != null && !future.isDone()) {
                    future.complete(agreement);
                }
                byte[] r = get(round);
                if (r != null) {
                    Preconditions.checkState(Arrays.equals(r, agreement));
                } else {
                    put(round, agreement);
                }
            }

            @Override
            public void failed(String error) {
                peerAcceptors.release(round);
                logger.error("proposal instance {} failed. error： {}", round, error);
            }
        };
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
