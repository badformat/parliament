package io.github.parliament.paxos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.acceptor.LocalAcceptor;

public class AcceptorTestFactory implements AcceptorFactory<String> {
    private ConcurrentHashMap<Integer, Collection<Acceptor<String>>> acceptors = new ConcurrentHashMap<>();

    @Override
    public synchronized Collection<Acceptor<String>> createPeersForRound(int round) {
        if (acceptors.contains(round)) {
            return acceptors.get(round);
        }

        ArrayList<Acceptor<String>> roundAcceptors = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            roundAcceptors.add(new LocalAcceptor<String>() {
                @Override
                public void decided(byte[] agreement) {

                }
            });
        }
        acceptors.put(round, roundAcceptors);

        return roundAcceptors;
    }

}
