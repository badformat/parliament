package io.github.parliament.paxos;

/**
 * 
 * @author zy
 *
 * @param <T> 可比较类型
 */
public interface Acceptor<T> {
    Prepare<T> prepare(Comparable<T> n);

    Accept<T> accept(Comparable<T> n, byte[] value);
}
