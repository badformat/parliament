package io.github.parliament.kv;

import io.github.parliament.EventProcessor;
import io.github.parliament.Persistence;
import io.github.parliament.ReplicateStateMachine;
import io.github.parliament.State;
import io.github.parliament.resp.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>
 * 可以使用不同一致性协议的kv服务，支持并发执行。
 * 目前使用pager完成持久化功能。
 * LRU缓存相关page实现本地持久化数据的高速缓存。
 * 使用redis resp协议完成交互。
 * 返回future，方便网络服务决定超时。
 * 提供相关任务（如一致性协议）的线程池，以便控制线程资源消耗。
 * <p>
 * 实现RSM接口，与一致性协议协调完成RSM功能。
 * RSM用例：
 * <ul>
 * <li>当其他节点的一个共识达成时，可收到通知并执行。</li>
 * <li>发现共识落后以后，追上其他节点的共识进度。</li>
 * <li>定时自动删除所有节点都已执行成功的共识。</li>
 * </ul>
 *
 * </p>
 *
 * @author zy
 */
public class KeyValueEngineImpl implements EventProcessor {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueEngineImpl.class);
    private static final String PUT_CMD = "put";
    private static final String GET_CMD = "get";
    private static final String DEL_CMD = "del";
    private ExecutorService executorService;
    private Path path;
    @Getter(AccessLevel.PACKAGE)
    private Persistence persistence;
    @Getter(AccessLevel.PACKAGE)
    private ReplicateStateMachine rsm;

    @Builder
    KeyValueEngineImpl(@NonNull Path path,
                       @NonNull ExecutorService executorService,
                       @NonNull ReplicateStateMachine rsm,
                       Persistence persistence)
            throws IOException {
        this.path = path;
        this.executorService = executorService;
        this.rsm = rsm;
        this.persistence = persistence;
    }

    public void start() throws IOException {
        rsm.start(this, executorService);
    }

    public Future<RespData> execute(byte[] bytes) throws Exception {
        RespDecoder decoder = new RespDecoder();
        decoder.decode(bytes);
        RespArray request = decoder.get();

        if (request.size() == 0) {
            throw new UnknownKeyValueCommand("empty command");
        }

        RespBulkString cmd = request.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);

        if (!(Objects.equals(cmdStr, PUT_CMD) || Objects.equals(cmdStr, GET_CMD) || Objects.equals(cmdStr, DEL_CMD))) {
            throw new UnknownKeyValueCommand("unknown command:" + cmdStr);
        }

        State state = rsm.state(bytes);
        CompletableFuture<State> future = rsm.submit(state);

        return future.thenApply((consensus) -> {
            try {
                if (!Arrays.equals(state.getUuid(), consensus.getUuid())) {
                    return RespError.withUTF8("共识冲突");
                }
                return RespDecoder.create().decode(consensus.getOutput()).get();
            } catch (Exception e) {
                logger.error("get execute result failed:", e);
                return RespError.withUTF8("get execute result failed:" + e.getClass().getName()
                        + ",message:" + e.getMessage());
            }
        });
    }

    <T> Future<T> execute1(byte[] bytes) throws Exception {
        return (Future<T>) execute(bytes);
    }

    @Override
    public byte[] process(byte[] content) {
        RespArray request = RespDecoder.create().decode(content).get();
        RespBulkString cmd = request.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);
        RespData ret = null;

        try {
            switch (cmdStr) {
                case PUT_CMD:
                    RespBulkString key = request.get(1);
                    RespBulkString value = request.get(2);
                    persistence.put(key.getContent(), value.getContent());
                    ret = RespInteger.with(1);
                    break;
                case GET_CMD:
                    key = request.get(1);
                    byte[] v = persistence.get(key.getContent());
                    ret = v == null ? RespBulkString.nullBulkString() : RespBulkString.with(v);
                    break;
                case DEL_CMD:
                    List<RespBulkString> keys = request.getDatas();
                    ret = RespInteger.with(del(keys.subList(1, keys.size())));
                    break;
                default:
                    ret = RespError.withUTF8("Unknown key value command: " + cmdStr);
            }
        } catch (IOException e) {
            logger.error("IO Exception:", e);
            ret = RespError.withUTF8("IO Exception.");
        }
        return ret.toBytes();
    }

    int del(List<RespBulkString> keys) throws IOException {
        int deleted = 0;
        for (RespBulkString key : keys) {
            if (persistence.remove(key.getContent())) {
                deleted++;
            }
        }
        return deleted;
    }
}
