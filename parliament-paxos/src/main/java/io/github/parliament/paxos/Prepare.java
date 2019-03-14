package io.github.parliament.paxos;

public class Prepare<T extends Comparable<T>> {
    private boolean ok;
    private T n;
    private T na;
    private byte[] va;

    private Prepare(T n, T na, byte[] va) {
        this.n = n;
        this.na = na;
        this.va = va;
        this.ok = true;
    }

    private Prepare(T n) {
        this.n = n;
    }

    public static <T2 extends Comparable<T2>> Prepare<T2> ok(T2 n, T2 na, byte[] va) {
        return new Prepare<T2>(n, na, va);
    }

    public static <T2 extends Comparable<T2>> Prepare<T2> reject(T2 n) {
        Prepare<T2> reject = new Prepare<T2>(n);
        reject.setOk(false);
        return reject;
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
