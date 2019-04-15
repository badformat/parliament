package io.github.parliament.paxos.proposer;

import java.util.concurrent.atomic.AtomicInteger;

public class TimestampSequence implements Sequence<String> {
    private static final AtomicInteger atomicIngeger = new AtomicInteger();

    @Override
    public String next() {
        long m = System.currentTimeMillis();
        String p = String.format("%04d", atomicIngeger.getAndIncrement() % 10000);
        return m + p;
    }

}
