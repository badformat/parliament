package io.github.parliament.paxos;

/**
 * 
 * @author zy
 *
 * @param <T> 可比较类型
 */
public interface Acceptor<T extends Comparable<T>> {
    Prepare<T> prepare(T n);

    Accept<T> accept(T n, byte[] value);
}
