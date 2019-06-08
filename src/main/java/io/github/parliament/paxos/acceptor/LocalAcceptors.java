package io.github.parliament.paxos.acceptor;

public interface LocalAcceptors {
    Acceptor create(int round) throws Exception;
}