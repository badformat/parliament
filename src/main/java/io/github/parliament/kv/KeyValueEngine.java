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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author zy
 */
public class KeyValueEngine implements StateTransfer {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueEngine.class);
    private static final String SET_CMD = "SET";
    private static final String GET_CMD = "GET";
    private static final String DEL_CMD = "DEL";
    private static final String RANGE_CMD = "RANGE";

    private ExecutorService executorService;
    @Getter(AccessLevel.PACKAGE)
    private Persistence persistence;
    @Getter(AccessLevel.PACKAGE)
    private ReplicateStateMachine rsm;

    @Builder
    KeyValueEngine(@NonNull ExecutorService executorService,
                   @NonNull ReplicateStateMachine rsm,
                   @NonNull Persistence persistence) {
        this.executorService = executorService;
        this.rsm = rsm;
        this.persistence = persistence;
    }

    public void start() throws IOException, ExecutionException {
        rsm.start(this, executorService);
    }

    public ByteBuffer execute(byte[] bytes, int timeout, TimeUnit unit) {
        try {
            RespDecoder decoder = new RespDecoder();
            decoder.decode(bytes);
            RespArray request = decoder.get();

            checkParams(request);

            ReplicateStateMachine.Input input = rsm.newState(bytes);
            CompletableFuture<ReplicateStateMachine.Output> future = rsm.submit(input);

            ReplicateStateMachine.Output output = future.get(timeout, unit);

            if (!Arrays.equals(input.getUuid(), output.getUuid())) {
                return RespError.withUTF8("共识冲突").toByteBuffer();
            }
            RespData resp = RespDecoder.create().decode(output.getContent()).get();
            return resp.toByteBuffer();
        } catch (Exception e) {
            return RespError.withUTF8("执行错误,ERROR:" + e.getMessage()).toByteBuffer();
        }
    }

    private void checkParams(RespArray request) throws UnknownKeyValueCommand, IOException {
        if (request.size() == 0) {
            throw new UnknownKeyValueCommand("empty command");
        }

        RespBulkString cmd = request.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8).toUpperCase();

        switch (cmdStr) {
            case SET_CMD:
                Preconditions.checkState(request.get(1) instanceof RespBulkString);
                Preconditions.checkState(request.get(2) instanceof RespBulkString);
                break;
            case GET_CMD:
                Preconditions.checkState(request.get(1) instanceof RespBulkString);
                break;
            case DEL_CMD:
                List<RespBulkString> keys = request.getDatas();
                for (RespBulkString key : keys) {
                    Preconditions.checkState(key != null);
                }
                break;
            case RANGE_CMD:
                Preconditions.checkState(3 == request.getDatas().size());
                break;
            default:
                throw new UnknownKeyValueCommand("unknown command:" + cmdStr);
        }
    }

    int del(List<RespBulkString> keys) throws IOException, ExecutionException {
        int deleted = 0;
        for (RespBulkString key : keys) {
            if (persistence.del(key.getContent())) {
                deleted++;
            }
        }
        return deleted;
    }

    @Override
    public ReplicateStateMachine.Output transform(ReplicateStateMachine.Input input) throws IOException, ExecutionException {
        RespArray request = RespDecoder.create().decode(input.getContent()).get();
        RespBulkString cmd = request.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8).toUpperCase();
        RespData resp = null;

        switch (cmdStr) {
            case SET_CMD:
                RespBulkString key = request.get(1);
                RespBulkString value = request.get(2);
                persistence.put(key.getContent(), value.getContent());
                resp = RespSimpleString.withUTF8("OK");
                break;
            case GET_CMD:
                key = request.get(1);
                byte[] v = persistence.get(key.getContent());
                resp = v == null ? RespBulkString.nullBulkString() : RespBulkString.with(v);
                break;
            case DEL_CMD:
                List<RespBulkString> keys = request.getDatas();
                resp = RespInteger.with(del(keys.subList(1, keys.size())));
                break;
            case RANGE_CMD:
                key = request.get(1);
                RespBulkString end = request.get(2);
                List<byte[]> r = persistence.range(key.getContent(), end.getContent());

                List<RespData> a = new ArrayList<>();
                r.forEach(bytes -> a.add(RespBulkString.with(bytes)));
                resp = RespArray.with(a);
                break;
            default:
                resp = RespError.withUTF8("Unknown key value command: " + cmdStr);
        }

        return ReplicateStateMachine.Output.builder()
                .content(resp.toBytes())
                .id(input.getId())
                .uuid(input.getUuid())
                .build();
    }
}
