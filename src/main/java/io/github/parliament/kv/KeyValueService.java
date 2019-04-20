package io.github.parliament.kv;

/**
 *
 * @author zy
 */
interface KeyValueService {
    void put(byte[] key, byte[] value);

    byte[] putIfAbsent(byte[] key, byte[] value);

    byte[] compareAndPut(byte[] key, byte[] expect, byte[] update);

    byte[] get(byte[] key);

    boolean del(byte[] key);
}