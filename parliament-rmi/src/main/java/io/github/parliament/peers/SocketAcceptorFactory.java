package io.github.parliament.peers;

import java.util.Collection;

import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;

public class SocketAcceptorFactory<T extends Comparable<T>> implements AcceptorFactory<T> {
    @Override
    public Collection<Acceptor<T>> createPeersForRound(int round) {
        return null;
    }
}
