package io.github.parliament.paxos.acceptor;

/**
 * 
 * @author zy
 *
 * @param <T> 可比较类型
 */
public interface Acceptor<T extends Comparable<T>> {
    Prepare<T> prepare(T n);

    Accept<T> accept(T n, byte[] value);

    void decided(byte[] agreement);

    T getNp();

    T getNa();

    byte[] getVa();
}
