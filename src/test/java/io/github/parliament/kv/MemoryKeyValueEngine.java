package io.github.parliament.kv;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;

/**
 *
 * @author zy
 */
public class MemoryKeyValueEngine implements KeyValueEngine {
    private Map<RespBulkString, RespBulkString> kvs = new ConcurrentHashMap<>();

    @Override
    public void start() throws Exception {

    }

    @Override
    public Future<RespData> execute(RespArray request) {
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