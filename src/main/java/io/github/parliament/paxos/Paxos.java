package io.github.parliament.paxos;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.collect.MapMaker;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.proposer.Proposer;
import io.github.parliament.paxos.proposer.Sequence;
import lombok.Getter;

public abstract class Paxos<T extends Comparable<T>> {
    private final ConcurrentMap<Integer, Proposer<T>> proposers = new MapMaker()
            .weakValues()
            .makeMap();

    @Getter
    protected ExecutorService executorService;

    @Getter
    protected Sequence<T> sequence;

    public void shutdown() throws Exception {
        executorService.shutdown();
    }

    public Future<Proposal> propose(int round, byte[] value) throws Exception {
        Proposer proposer = proposers.computeIfAbsent(round, (r) -> new Proposer<T>(getAcceptors(r), sequence, value));

        return executorService.submit(() -> {
            byte[] agreement = proposer.propose();
            return Proposal.builder().agreement(agreement).round(round).build();
        });
    }

    protected abstract Collection<Acceptor<T>> getAcceptors(int round);
}
