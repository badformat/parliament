package io.github.parliament.paxos.acceptor;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public abstract class LocalAcceptor implements Acceptor {
    @Getter
    protected int round;
    @Getter
    private String np;
    @Getter
    private String na;
    @Getter
    private byte[] va;

    protected LocalAcceptor(int round) {
        this.round = round;
    }

    @Override
    public synchronized Prepare prepare(String n) {
        if (np == null || n.compareTo(np) > 0) {
            np = n;
            return Prepare.ok(n, na, va);
        }
        return Prepare.reject(n);
    }

    @Override
    public synchronized Accept accept(String n, byte[] value) {
        if (np == null) {
            return Accept.reject(n);
        }
        if (n.compareTo(np) >= 0) {
            np = n;
            na = n;
            va = value;
            return Accept.ok(n);
        }
        return Accept.reject(n);
    }

    @Override
    abstract public void decide(byte[] agreement) throws Exception;
}
