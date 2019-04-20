package io.github.parliament.paxos.acceptor;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public abstract class LocalAcceptor<T extends Comparable<T>> implements Acceptor<T> {
    @Getter
    private T      np;
    @Getter
    private T      na;
    @Getter
    private byte[] va;

    @Override
    public Prepare<T> prepare(T n) {
        if (np == null || n.compareTo(np) >= 0) {
            np = n;
            return Prepare.<T>ok(n, na, va);
        }
        return Prepare.<T>reject(n);
    }

    @Override
    public Accept<T> accept(T n, byte[] value) {
        if (np == null) {
            return Accept.<T>reject(n);
        }
        if (n.compareTo(np) >= 0) {
            np = n;
            na = n;
            va = value;
            return Accept.<T>ok(n);
        }
        return Accept.<T>reject(n);
    }

    @Override
    abstract public void decide(byte[] agreement) throws Exception;
}
