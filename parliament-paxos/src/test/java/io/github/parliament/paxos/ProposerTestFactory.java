package io.github.parliament.paxos;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import com.google.inject.Inject;

import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.proposer.Proposer;
import io.github.parliament.paxos.proposer.ProposerFactory;
import io.github.parliament.paxos.proposer.Sequence;

public class ProposerTestFactory implements ProposerFactory<String> {
    private ConcurrentHashMap<Integer, Proposer<String>> proposers = new ConcurrentHashMap<>();
    @Inject
    private AcceptorFactory<String> acceptorFactory;
    @Inject
    private Sequence<String> sequence;

    @Override
    public synchronized Proposer<String> createProposerForRound(int round, byte[] value) {
        if (proposers.contains(round)) {
            return proposers.get(round);
        }
        Collection<Acceptor<String>> accs = acceptorFactory.createPeersForRound(round);
        Proposer<String> proposer = new Proposer<String>(accs, sequence, value);
        proposers.put(round, proposer);
        return proposer;
    }
}
