package io.github.parliament.paxos.acceptor;

import lombok.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@EqualsAndHashCode
@ToString
public abstract class LocalAcceptor implements Acceptor {
    @Getter
    protected int round;
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private String np;
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private String na;
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private byte[] va;

    protected LocalAcceptor(int round) {
        this.round = round;
    }

    @Override
    public synchronized Prepare prepare(String n) throws Exception {
        if (np == null || n.compareTo(np) > 0) {
            np = n;
            persistence();
            return Prepare.ok(n, na, va);
        }
        return Prepare.reject(n);
    }

    @Override
    public synchronized Accept accept(String n, byte[] value) throws Exception {
        if (np == null) {
            return Accept.reject(n);
        }
        if (n.compareTo(np) >= 0) {
            np = n;
            na = n;
            va = value;
            persistence();
            return Accept.ok(n);
        }
        return Accept.reject(n);
    }

    @Override
    abstract public void decide(byte[] agreement) throws Exception;

    abstract public void persistence() throws IOException, ExecutionException;
}
