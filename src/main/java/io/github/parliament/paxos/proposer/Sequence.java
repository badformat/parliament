package io.github.parliament.paxos.proposer;

public interface Sequence<T extends Comparable<T>> {
    T next();
}
