package io.github.parliament.kv;

/**
 *
 * @author zy
 */
public interface Store {
    void insert(byte[] key, byte[] value);

    void del(byte[] key, byte[] value);

    byte[] get(byte[] key);

    byte[] range(byte[] begin, byte[] end, int limit);
}