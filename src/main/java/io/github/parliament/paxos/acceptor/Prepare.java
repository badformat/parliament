package io.github.parliament.paxos.acceptor;

public class Prepare {
    private boolean ok;
    private String n;
    private String na;
    private byte[] va;

    private Prepare(String n, String na, byte[] va) {
        this.n = n;
        this.na = na;
        this.va = va;
        this.ok = true;
    }

    private Prepare(String n) {
        this.n = n;
    }

    public static Prepare ok(String n, String na, byte[] va) {
        return new Prepare(n, na, va);
    }

    public static Prepare reject(String n) {
        Prepare reject = new Prepare(n);
        reject.setOk(false);
        return reject;
    }

    public boolean isOk() {
        return ok;
    }

    void setOk(boolean ok) {
        this.ok = ok;
    }

    public String getN() {
        return n;
    }

    void setN(String n) {
        this.n = n;
    }

    public String getNa() {
        return na;
    }

    void setNa(String na) {
        this.na = na;
    }

    public byte[] getVa() {
        return va;
    }

    void setVa(byte[] va) {
        this.va = va;
    }

}
