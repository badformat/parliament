package io.github.parliament.paxos.acceptor;

public interface AcceptorFactory<T extends Comparable<T>> {
    Acceptor<T> createLocalAcceptorFor(int round) throws Exception;

    void shutdown() throws Exception;
}