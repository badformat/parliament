package io.github.parliament.paxos.acceptor;

/**
 * 
 * @author zy
 *
 * @param <T> 可比较类型
 */
public interface Acceptor<T extends Comparable<T>> {
    Prepare<T> prepare(T n) throws Exception;

    Accept<T> accept(T n, byte[] value) throws Exception;

    void decide(byte[] agreement) throws Exception;
}
