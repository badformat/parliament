package io.github.parliament;

import java.io.IOException;

/**
 * @author zy
 */
public interface Persistence {
    void put(byte[] bytes, byte[] array) throws IOException;

    byte[] get(byte[] key) throws IOException;

    boolean remove(byte[] key) throws IOException;
}
