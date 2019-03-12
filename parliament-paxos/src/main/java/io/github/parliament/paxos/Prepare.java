package io.github.parliament.paxos;

public class Prepare<T extends Comparable<T>> {
    private boolean ok;
    private T n;
    private T na;
    private byte[] va;

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

    public T getNa() {
        return na;
    }

    void setNa(T na) {
        this.na = na;
    }

    public byte[] getVa() {
        return va;
    }

    void setVa(byte[] va) {
        this.va = va;
    }

}
