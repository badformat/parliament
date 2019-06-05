package io.github.parliament.kv;

import io.github.parliament.Persistence;
import io.github.parliament.ReplicateStateMachine;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespDecoder;
import lombok.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author zy
 */
public class MemoryKeyValueEngine extends KeyValueEngine {
    private Map<RespBulkString, RespBulkString> kvs = new ConcurrentHashMap<>();

    MemoryKeyValueEngine(@NonNull ExecutorService executorService,
                         @NonNull ReplicateStateMachine rsm,
                         @NonNull Persistence persistence) {
        super(executorService, rsm, persistence);
    }

    @Override
    public void start() {

    }

    @Override
    public Future<RespData> execute(byte[] bytes) {
        RespDecoder decoder = new RespDecoder();
        decoder.decode(bytes);
        RespArray request = decoder.get();
        RespBulkString c = request.get(0);
        String cmd = new String(c.getContent(), StandardCharsets.UTF_8);
        switch (cmd) {
            case "put":
                RespBulkString key = request.get(1);
                RespBulkString value = request.get(2);
                kvs.put(key, value);
                return CompletableFuture.completedFuture(value);
            case "get":
                key = request.get(1);
                RespBulkString v = kvs.get(key);
                return CompletableFuture.completedFuture(v);
            default:
                return null;
        }
    }
}