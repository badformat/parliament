package io.github.parliament.paxos;

public class LocalAcceptor<T extends Comparable<T>> implements Acceptor<T> {
    private T np;
    private T na;
    private byte[] va;

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
