package io.github.parliament.kv;

import com.google.common.base.Preconditions;
import io.github.parliament.*;
import io.github.parliament.resp.*;
import io.github.parliament.skiplist.SkipList;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 *
 * @author zy
 */
public class KeyValueEngine implements StateTransfer {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueEngine.class);
    private static final String PUT_CMD = "put";
    private static final String GET_CMD = "get";
    private static final String DEL_CMD = "del";
    private static final String RANGE_CMD = "range";

    private ExecutorService executorService;
    @Getter(AccessLevel.PACKAGE)
    private SkipList skipList;
    @Getter(AccessLevel.PACKAGE)
    private ReplicateStateMachine rsm;

    @Builder
    KeyValueEngine(@NonNull ExecutorService executorService,
                   @NonNull ReplicateStateMachine rsm,
                   @NonNull SkipList skipList) {
        this.executorService = executorService;
        this.rsm = rsm;
        this.skipList = skipList;
    }

    public void start() throws IOException, ExecutionException {
        rsm.start(this, executorService);
    }

    public Future<RespData> submit(byte[] bytes) throws IOException, ExecutionException {
        try {
            RespDecoder decoder = new RespDecoder();
            decoder.decode(bytes);
            RespArray request = decoder.get();

            checkSubmitted(request);

            Input input = rsm.newState(bytes);
            CompletableFuture<Output> future = rsm.submit(input);
            return future.thenApply((output) -> {
                try {
                    if (!Arrays.equals(input.getUuid(), output.getUuid())) {
                        return RespError.withUTF8("共识冲突");
                    }
                    return RespDecoder.create().decode(output.getContent()).get();
                } catch (Exception e) {
                    logger.error("get submit result failed:", e);
                    return RespError.withUTF8("get submit result failed:" + e.getClass().getName()
                            + ",message:" + e.getMessage());
                }
            });
        } catch (UnknownKeyValueCommand e) {
            return CompletableFuture.completedFuture(RespError.withUTF8("Unknown Command."));
        }


    }

    private void checkSubmitted(RespArray request) throws UnknownKeyValueCommand, IOException {
        if (request.size() == 0) {
            throw new UnknownKeyValueCommand("empty command");
        }

        RespBulkString cmd = request.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);

        switch (cmdStr) {
            case PUT_CMD:
                Preconditions.checkState(request.get(1) instanceof RespBulkString);
                Preconditions.checkState(request.get(2) instanceof RespBulkString);
                break;
            case GET_CMD:
                Preconditions.checkState(request.get(1) instanceof RespBulkString);
                break;
            case DEL_CMD:
                List<RespBulkString> keys = request.getDatas();
                for (RespBulkString key : keys) {
                    Preconditions.checkState(key instanceof RespBulkString);
                }
                break;
            case RANGE_CMD:
                Preconditions.checkState(3 == request.getDatas().size());
                break;
            default:
                throw new UnknownKeyValueCommand("unknown command:" + cmdStr);
        }
    }

    <T> Future<T> submit1(byte[] bytes) throws Exception {
        return (Future<T>) submit(bytes);
    }

    int del(List<RespBulkString> keys) throws IOException, ExecutionException {
        int deleted = 0;
        for (RespBulkString key : keys) {
            if (skipList.del(key.getContent())) {
                deleted++;
            }
        }
        return deleted;
    }

    @Override
    public Output transform(Input input) throws IOException, ExecutionException {
        RespArray request = RespDecoder.create().decode(input.getContent()).get();
        RespBulkString cmd = request.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);
        RespData resp = null;

        switch (cmdStr) {
            case PUT_CMD:
                RespBulkString key = request.get(1);
                RespBulkString value = request.get(2);
                skipList.put(key.getContent(), value.getContent());
                resp = RespInteger.with(1);
                break;
            case GET_CMD:
                key = request.get(1);
                byte[] v = skipList.get(key.getContent());
                resp = v == null ? RespBulkString.nullBulkString() : RespBulkString.with(v);
                break;
            case DEL_CMD:
                List<RespBulkString> keys = request.getDatas();
                resp = RespInteger.with(del(keys.subList(1, keys.size())));
                break;
            case RANGE_CMD:
                key = request.get(1);
                RespBulkString end = request.get(2);
                List<byte[]> r = skipList.range(key.getContent(), end.getContent());

                List<RespData> a = new ArrayList<>();
                r.forEach(bytes -> {
                    a.add(RespBulkString.with(bytes));
                });
                resp = RespArray.with(a);
                break;
            default:
                resp = RespError.withUTF8("Unknown key value command: " + cmdStr);
        }

        return Output.builder()
                .content(resp.toBytes())
                .id(input.getId())
                .uuid(input.getUuid())
                .build();
    }
}
