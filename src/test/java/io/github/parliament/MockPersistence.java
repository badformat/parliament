package io.github.parliament;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MockPersistence implements Persistence {
    ConcurrentHashMap<List<Byte>, byte[]> map = new ConcurrentHashMap<>();

    @Override
    public void put(byte[] key, byte[] value) {
        map.put(toList(key), value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return map.get(toList(key));
    }

    @Override
    public boolean del(byte[] key) throws IOException {
        return map.remove(toList(key)) != null;
    }

    List<Byte> toList(byte[] a) {
        ArrayList<Byte> l = new ArrayList<>();
        for (byte b : a) {
            l.add(b);
        }
        return l;
    }
}
