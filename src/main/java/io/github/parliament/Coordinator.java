package io.github.parliament;

import java.util.concurrent.Future;

/**
 * @author zy
 **/
public interface Coordinator {
    void coordinate(int id, byte[] content);

    Future<byte[]> instance(int id);

    int max();

    void forget(int before);
}
