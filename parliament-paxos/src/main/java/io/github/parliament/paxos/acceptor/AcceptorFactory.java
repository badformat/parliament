package io.github.parliament.paxos.acceptor;

import java.util.Collection;

public interface AcceptorFactory<T extends Comparable<T>> {
    Collection<Acceptor<T>> createPeersForRound(int round);
}
