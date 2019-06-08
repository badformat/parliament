package io.github.parliament;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 删除已处理共识的流程：
 * <ul>
 * <li>咨询所有rsm已处理事件的最大编号</li>
 * <li>从以上编号中取最小</li>
 * <li>删除该最小值之前的事件</li>
 * </ul>
 *
 * @author zy
 **/
public interface Coordinator {
    void coordinate(int id, byte[] content) throws ExecutionException;

    Future<byte[]> instance(int id) throws IOException, ExecutionException;

    void instance(int round, byte[] content) throws IOException;

    int min();

    int done() throws IOException;

    void done(int done) throws IOException;

    int max();

    void max(int m) throws IOException;

    void forget(int before) throws IOException;

    byte[] get(int round);
}
