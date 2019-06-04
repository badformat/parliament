package io.github.parliament;

public interface Sequence<T extends Comparable<T>> {
    T next();

    void set(T n);

    T current();
}
