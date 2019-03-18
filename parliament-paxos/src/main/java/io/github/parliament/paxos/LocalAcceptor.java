package io.github.parliament.paxos;

import lombok.Getter;

public class LocalAcceptor<T extends Comparable<T>> implements Acceptor<T> {
    @Getter
    private T np;
    @Getter
    private T na;
    @Getter
    private byte[] va;
    // TODO 持久化策略协作对象

    @Override
    public Prepare<T> prepare(T n) {
        if (np == null || n.compareTo(np) > 0) {
            np = n;
            return Prepare.<T>ok(n, na, va);
        }
        return Prepare.<T>reject(n);
    }

    @Override
    public Accept<T> accept(T n, byte[] value) {
        if (np == null || n.compareTo(np) >= 0) {
            np = n;
            na = n;
            va = value;
            return Accept.<T>ok(n);
        }
        return Accept.<T>reject(n);
    }

    @Override
    public void decided(byte[] agreement) {
        // TODO Auto-generated method stub

    }

}
