package io.github.parliament.kv;

import io.github.parliament.ConsensusService;
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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * @author zy
 */
public class KeyValueEngineImpl {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueEngineImpl.class);
    private static final String PUT_CMD = "put";
    private static final String GET_CMD = "get";
    private static final String DEL_CMD = "del";
    private ExecutorService executorService;
    private ConsensusService consensusService;
    private Path path;
    @Getter(AccessLevel.PACKAGE)
    private Pager pager;

    @Builder
    KeyValueEngineImpl(@NonNull Path path, @NonNull ExecutorService executorService) {
        this.path = path;
        this.executorService = executorService;
    }

    public RespData execute(byte[] bytes) throws Exception {
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

//        consensusService.buildConsensus(bytes);

        RespData ret = null;
        switch (cmdStr) {
            case PUT_CMD:
                RespBulkString key = request.get(1);
                RespBulkString value = request.get(2);
                if (pager.get(key.getContent()) != null) {
                    pager.remove(key.getContent());
                }
                pager.insert(key.getContent(), value.getContent());
                ret = RespInteger.with(1);
                break;
            case GET_CMD:
                key = request.get(1);
                byte[] v = pager.get(key.getContent());
                ret = v == null ? RespBulkString.nullBulkString() : RespBulkString.with(v);
                break;
            case DEL_CMD:
                List<RespBulkString> keys = request.getDatas();
                ret = RespInteger.with(del(keys.subList(1, keys.size())));
                break;
            default:
                throw new UnknownKeyValueCommand("Unknown key value service: " + cmdStr);
        }
        return ret;
    }

    int del(List<RespBulkString> keys) throws IOException {
        int deleted = 0;
        for (RespBulkString key : keys) {
            if (pager.remove(key.getContent())) {
                deleted++;
            }
        }
        return deleted;
    }
}
