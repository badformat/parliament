package io.github.parliament.paxos;

import io.github.parliament.Sequence;

import java.util.concurrent.atomic.AtomicInteger;

public class TimestampSequence implements Sequence<String> {
    private final AtomicInteger atomicInteger = new AtomicInteger();
    private String current = "";

    @Override
    public String next() {
        long m = System.currentTimeMillis();
        String p = String.format("%04d", atomicInteger.getAndIncrement() % 10000);
        current = m + p;
        return current;
    }

    @Override
    public void set(String n) {
    }

    @Override
    public String current() {
        return current;
    }
}
