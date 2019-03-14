package io.github.parliament.paxos;

public class TimestampIpSequence implements Sequence<String> {

    @Override
    public String next() {
        long m = System.currentTimeMillis();

        return String.valueOf(m);
    }

}
