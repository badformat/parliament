package io.github.parliament;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author zy
 */
public interface Persistence {
    void put(byte[] bytes, byte[] array) throws IOException, ExecutionException;

    byte[] get(byte[] key) throws IOException, ExecutionException;

    boolean del(byte[] key) throws IOException, ExecutionException;

    List<byte[]> range(byte[] min, byte[] max) throws IOException, ExecutionException;
}
