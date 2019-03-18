package io.github.parliament;

import java.util.Collection;

import io.github.parliament.paxos.Acceptor;

public class AcceptorFactory<T extends Comparable<T>> {

    Collection<Acceptor<T>> makeAllForRound(long round) {
        return null;
    }
    
    Acceptor<T> makeLocalForRound(long round) {
        return null;
    }
}
