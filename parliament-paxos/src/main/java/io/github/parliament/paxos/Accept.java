package io.github.parliament.paxos;

public class Accept<T extends Comparable<?>> {
    private boolean ok;
    private T n;

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
}
