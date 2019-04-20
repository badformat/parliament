package io.github.parliament.paxos.acceptor;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 *
 * @author zy
 */
@EqualsAndHashCode
@ToString
@Builder
public class ExceptionDeferAcceptor<T extends Comparable<T>> implements Acceptor<T> {
    private Exception exception;

    @Override
    public Prepare<T> prepare(T n) {
        return Prepare.reject(n);
    }

    @Override
    public Accept<T> accept(T n, byte[] value) {
        return Accept.reject(n);
    }

    @Override
    public void decide(byte[] agreement) throws Exception {
        throw exception;
    }
}