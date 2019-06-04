package io.github.parliament;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class MockPersistence implements Persistence {
    ConcurrentHashMap<String, byte[]> map = new ConcurrentHashMap<>();

    @Override
    public void put(byte[] key, byte[] value) {
        map.put(new String(key), value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return map.get(new String(key));
    }

    @Override
    public boolean remove(byte[] key) throws IOException {
        return map.remove(new String(key)) != null;
    }
}
