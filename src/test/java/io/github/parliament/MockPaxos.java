package io.github.parliament;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

class MockPaxos implements Coordinator {
    private ConcurrentHashMap<Integer, byte[]> states = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, CompletableFuture<byte[]>> waiting = new ConcurrentHashMap<>();
    private volatile int max = 0;

    @Override
    public void coordinate(int id, byte[] content) {
        states.put(id, content);
        max = Math.max(id, max);

        waiting.putIfAbsent(id, new CompletableFuture<>());
        waiting.get(id).complete(content);
    }

    @Override
    public Future<byte[]> instance(int id) {
        if (states.containsKey(id)) {
            return CompletableFuture.completedFuture(states.get(id));
        }
        waiting.put(id, new CompletableFuture<>());
        return waiting.get(id);
    }

    @Override
    public int min() {
        return 0;
    }

    @Override
    public int done() throws IOException {
        return 0;
    }

    @Override
    public void done(int done) throws IOException {

    }

    @Override
    public int max() {
        return max;
    }

    @Override
    public void max(int m) {
        max = m;
    }

    @Override
    public void forget(int before) {

    }

    @Override
    public byte[] get(int round) {
        return new byte[0];
    }

    @Override
    public void learn(int round) {

    }

    void clear() {
        states.clear();
        waiting.clear();
    }
}
