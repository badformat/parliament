package io.github.parliament.paxos;

public interface Sequence<T extends Comparable<T>> {
    T next();
}
