package io.github.parliament.paxos.proposer;

import io.github.parliament.Sequence;

import java.util.concurrent.atomic.AtomicInteger;

public class TimestampSequence implements Sequence<String> {
    private static final AtomicInteger atomicIngeger = new AtomicInteger();
    private String current = "";

    @Override
    public String next() {
        long m = System.currentTimeMillis();
        String p = String.format("%04d", atomicIngeger.getAndIncrement() % 10000);
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
