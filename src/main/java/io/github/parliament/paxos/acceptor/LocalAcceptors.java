package io.github.parliament.paxos.acceptor;

import java.util.Collection;

public interface LocalAcceptors {
    Acceptor create(int round) throws Exception;
}