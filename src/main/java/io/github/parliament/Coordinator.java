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
    Future<byte[]> coordinate(int id, byte[] content) throws ExecutionException, IOException;

    Future<byte[]> instance(int id) throws ExecutionException, IOException;
    
    int min();

    int done() throws IOException;

    void done(int done) throws IOException, ExecutionException;

    int max();

    void max(int m) throws IOException, ExecutionException;

    void forget(int before) throws IOException, ExecutionException;

    byte[] get(int id) throws IOException, ExecutionException;

    void learn(int id) throws IOException, ExecutionException;

    void register(ReplicateStateMachine rsm);
}
