package io.github.parliament.server;

import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;

public interface PaxosAcceptorServerInterface<T extends Comparable<T>> {

    Prepare<T> prepare(int round, T n);

    Accept<T> accept(int round, T n, byte[] value);

    void decided(int round, byte[] agreement);
}
