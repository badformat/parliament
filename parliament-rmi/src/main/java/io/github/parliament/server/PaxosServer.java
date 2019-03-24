package io.github.parliament.server;

import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;

public class PaxosServer<T extends Comparable<T>> implements PaxosServerInterface<T> {
    @Override
    public Prepare<T> prepare(int round, T n) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Accept<T> accept(int round, T n, byte[] value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void decided(int round, byte[] agreement) {
        // TODO Auto-generated method stub

    }

}
