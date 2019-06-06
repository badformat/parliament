package io.github.parliament.paxos.acceptor;

public class Accept {
    private boolean ok;
    private String n;

    private Accept(String n, boolean ok) {
        this.n = n;
        this.ok = ok;
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

    public static Accept ok(String n) {
        return new Accept(n, true);
    }

    public static Accept reject(String n) {
        return new Accept(n, false);
    }
}
