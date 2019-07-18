package io.github.parliament;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author zy
 */
public interface Persistence {
    void put(byte[] bytes, byte[] array) throws IOException, ExecutionException;

    byte[] get(byte[] key) throws IOException, ExecutionException;

    boolean del(byte[] key) throws IOException, ExecutionException;
}
