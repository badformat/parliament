package io.github.parliament.paxos.acceptor;

public class Accept<T extends Comparable<T>> {
    private boolean ok;
    private T n;

    private Accept(T n, boolean ok) {
        this.n = n;
        this.ok = ok;
    }

    public boolean isOk() {
        return ok;
    }

    void setOk(boolean ok) {
        this.ok = ok;
    }

    public T getN() {
        return n;
    }

    void setN(T n) {
        this.n = n;
    }

    public static <T2 extends Comparable<T2>> Accept<T2> ok(T2 n) {
        return new Accept<>(n, true);
    }

    public static <T2 extends Comparable<T2>> Accept<T2> reject(T2 n) {
        return new Accept<>(n, false);
    }
}
