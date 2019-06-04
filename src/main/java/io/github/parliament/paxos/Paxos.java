package io.github.parliament.paxos;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.MapMaker;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.proposer.Proposer;
import io.github.parliament.Sequence;
import lombok.Getter;

public abstract class Paxos<T extends Comparable<T>> {
    private final ConcurrentMap<Integer, Proposer<T>> proposers = new MapMaker()
            .weakValues()
            .makeMap();

    @Getter
    protected volatile ExecutorService executorService;

    @Getter
    protected Sequence<T> sequence;

    public Proposal propose(int round, byte[] value) {
        Proposer proposer = proposers.computeIfAbsent(round, (r) -> new Proposer<>(getAcceptors(r), sequence, value));

        return Proposal.builder().agreement(executorService.submit(proposer::propose)).round(round).build();
    }

    protected abstract Collection<Acceptor<T>> getAcceptors(int round);
}
