package io.github.parliament;

import java.util.concurrent.atomic.AtomicInteger;

public class IntegerSequence implements Sequence<Integer> {
    private AtomicInteger atomicInteger = new AtomicInteger();

    @Override
    public Integer next() {
        return atomicInteger.getAndIncrement();
    }

    @Override
    public void set(Integer n) {
        atomicInteger.set(n);
    }

    @Override
    public Integer current() {
        return atomicInteger.get();
    }
}
