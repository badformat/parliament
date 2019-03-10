package io.github.parliament.paxos;

public class Accept<T> {
    private boolean ok;
    private Comparable<T> n;

    public boolean isOk() {
        return ok;
    }

    void setOk(boolean ok) {
        this.ok = ok;
    }

    public Comparable<T> getN() {
        return n;
    }

    void setN(Comparable<T> n) {
        this.n = n;
    }
}
