package io.github.parliament.paxos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.paxos.proposer.TimestampSequence;

public class PaxosSimple extends Paxos<String> {
    private ConcurrentHashMap<Integer, Collection<Acceptor<String>>> acceptors = new ConcurrentHashMap<>();

    public PaxosSimple() {
        this.executorService = Executors.newFixedThreadPool(15);
        this.sequence = new TimestampSequence();
    }

    @Override
    public synchronized Collection<Acceptor<String>> getAcceptors(int round) {
        if (acceptors.contains(round)) {
            return acceptors.get(round);
        }

        ArrayList<Acceptor<String>> roundAcceptors = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            roundAcceptors.add(new LocalAcceptor<String>() {
                @Override
                public void decide(byte[] agreement) {

                }
            });
        }
        acceptors.put(round, roundAcceptors);

        return roundAcceptors;
    }

}
