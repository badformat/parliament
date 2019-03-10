package io.github.parliament.paxos;

public class Prepare<T> {
    private boolean ok;
    private Comparable<T> n;
    private Comparable<T> na;
    private byte[] va;

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

    public Comparable<T> getNa() {
        return na;
    }

    void setNa(Comparable<T> na) {
        this.na = na;
    }

    public byte[] getVa() {
        return va;
    }

    void setVa(byte[] va) {
        this.va = va;
    }

}
