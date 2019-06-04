package io.github.parliament;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

class MockCoordinator implements Coordinator {
    private ConcurrentHashMap<Integer, byte[]> states = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, CompletableFuture<byte[]>> waiting = new ConcurrentHashMap<>();
    private volatile int max = 0;

    @Override
    public void coordinate(int id, byte[] content) {
        states.put(id, content);
        max = Math.max(id, max);
        if (waiting.containsKey(id)) {
            waiting.get(id).complete(content);
            waiting.remove(id);
        }
    }

    @Override
    public Future<byte[]> instance(int id) {
        if (states.containsKey(id)) {
            return CompletableFuture.completedFuture(states.get(id));
        }
        waiting.put(id, new CompletableFuture<byte[]>());
        return waiting.get(id);
    }

    @Override
    public int max() {
        return max;
    }

    @Override
    public void forget(int before) {

    }

    void clear() {
        states.clear();
        waiting.clear();
    }
}
