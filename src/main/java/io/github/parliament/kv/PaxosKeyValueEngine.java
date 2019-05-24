package io.github.parliament.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespDecoder;
import io.github.parliament.resp.RespError;
import io.github.parliament.resp.RespInteger;
import io.github.parliament.rsm.PaxosReplicateStateMachine;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author zy
 */
public class PaxosKeyValueEngine implements KeyValueEngine, Runnable {
    private static final Logger                     logger  = LoggerFactory.getLogger(PaxosKeyValueEngine.class);
    private static final String                     PUT_CMD = "put";
    private static final String                     GET_CMD = "get";
    private static final String                     DEL_CMD = "del";
    @Getter(AccessLevel.PACKAGE)
    private volatile     PaxosReplicateStateMachine rsm;
    @Getter(AccessLevel.PACKAGE)
    private              Pager                      pager;
    @Getter(AccessLevel.PACKAGE)
    private volatile     int                        cursor  = 0;
    private volatile     BlockingQueue<RequestWork> works   = new ArrayBlockingQueue<>(200);
    private volatile     Thread                     worker;
    @Getter(AccessLevel.PACKAGE)
    private volatile     Path                       path;
    private              Path                       cursorPath;
    private volatile     boolean                    started = false;

    @Builder
    private static class RequestWork {
        @Getter
        @NonNull
        private byte[]                      request;
        @Getter
        @NonNull
        private CompletableFuture<RespData> future;
    }

    @Builder
    PaxosKeyValueEngine(@NonNull Path path, @NonNull PaxosReplicateStateMachine rsm) throws IOException {
        this.path = path;
        this.rsm = rsm;
        cursorPath = path.resolve("cursor");
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
        if (!Files.exists(cursorPath)) {
            Files.createFile(cursorPath);
        }
        pager = Pager.builder().path(path.toString()).build();
    }

    @Override
    public void start() throws Exception {
        synchronized (this) {
            Preconditions.checkState(!started, "already started");
            recoverCursor();
            worker = new Thread(this);
            worker.start();
            if (!rsm.isStarted()) {
                rsm.start();
            }
            started = true;
        }
    }

    public void shutdown() throws IOException {
        synchronized (this) {
            Preconditions.checkState(started, "not started,can't shutdown.");
            worker.interrupt();
            worker = null;
            if (rsm.isStarted()) {
                rsm.shutdown();
            }
            started = false;
        }
    }

    //TODO using thread pool， every client has a works queue?
    @Override
    public void run() {
        for (; ; ) {
            if (Thread.currentThread().isInterrupted()) {
                logger.info("kv engine executor thread interrupted, exit.");
                return;
            }
            try {
                RequestWork work = works.poll(10, TimeUnit.SECONDS);
                if (work == null) {
                    continue;
                }

                byte[] request = work.getRequest();
                Proposal proposal = rsm.propose(request);
                int round = proposal.getRound();

                CompletableFuture<RespData> f = work.getFuture();
                try {
                    catchUp(round);
                } catch (Exception e) {
                    logger.error("catch up failed.", e);
                    f.complete(error("catch up failed"));
                }

                byte[] value = proposal.getAgreement().get();
                RespData resp;
                if (Arrays.equals(value, request)) {
                    resp = execute(round, value);
                } else {
                    execute(round, value);
                    logger.info("共识冲突 {}", round);
                    resp = RespError.withUTF8("consensus collision occurs.Please check and retry.");
                }

                f.complete(resp);
            } catch (InterruptedException e) {
                logger.error("kv engine executor thread interrupted, exits.");
                return;
            } catch (Exception e) {
                logger.error("thread error.", e);
            }
        }
    }

    @Override
    public Future<RespData> execute(final RespArray request) {
        Preconditions.checkState(started, "not started.");

        CompletableFuture<RespData> f = new CompletableFuture<>();

        RespBulkString cmd = request.get(0);
        if (!support(cmd)) {
            f.complete(error("unknown command:" + new String(cmd.getContent())));
            return f;
        }

        if (!argumentsIsValid(cmd, request)) {
            f.complete(error("arguments is invalid:" + new String(cmd.getContent())));
            return f;
        }

        works.add(RequestWork.builder().future(f).request(request.toBytes()).build());
        return f;
    }

    private void catchUp(int round) throws Exception {
        while (cursor < round) {
            Optional<byte[]> proprosal = rsm.proposal(cursor);
            if (!proprosal.isPresent()) {
                rsm.sync(cursor).get();
                proprosal = rsm.proposal(cursor);
            }

            if (!proprosal.isPresent()) {
                rsm.propose(cursor, RespArray.with().toBytes());
                logger.warn("proposal for round {} not found.", cursor);
                throw new PreviousRequestNotFoundExcpetion("proposal for round " + cursor + " not exists,propose a empty array.");
            }

            execute(cursor, proprosal.get());
        }
    }

    private RespData execute(int round, byte[] request) throws UnknownKeyValueCommand, IOException, DuplicateKeyException {
        RespDecoder decoder = new RespDecoder();
        decoder.decode(request);
        RespArray consensusReq = decoder.get();
        if (consensusReq.size() == 0) {
            return nullBulk();
        }
        RespBulkString cmd = consensusReq.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);
        RespData ret = null;
        switch (cmdStr) {
            case PUT_CMD:
                RespBulkString key = consensusReq.get(1);
                RespBulkString value = consensusReq.get(2);
                if (pager.get(key.getContent()) != null) {
                    pager.remove(key.getContent());
                }
                pager.insert(key.getContent(), value.getContent());
                ret = RespInteger.with(1);
                break;
            case GET_CMD:
                key = consensusReq.get(1);
                byte[] v = pager.get(key.getContent());
                ret = v == null ? nullBulk() : RespBulkString.with(v);
                break;
            case DEL_CMD:
                List<RespBulkString> keys = consensusReq.getDatas();
                ret = RespInteger.with(del(keys.subList(1, keys.size())));
                break;
            default:
                logger.error("not found cursor file, set cursor to 0");
                throw new UnknownKeyValueCommand("Unknown key value service: " + cmdStr);
        }
        updateCursor(round + 1);
        return ret;
    }

    private void recoverCursor() throws IOException {
        try (SeekableByteChannel ch = Files.newByteChannel(cursorPath, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocate(4);
            int i = 0;
            do {
                i = ch.read(dst);
            } while (i != -1 && i != 0);
            dst.flip();
            if (dst.limit() == 4) {
                cursor = dst.getInt();
            }
        }
    }

    private void updateCursor(int i) throws IOException {
        cursor = i;
        try (SeekableByteChannel ch = Files.newByteChannel(cursorPath, StandardOpenOption.WRITE)) {
            ByteBuffer src = ByteBuffer.allocate(4);
            src.putInt(i);
            src.flip();
            while (src.hasRemaining()) {
                ch.write(src);
            }
        }
    }

    private int del(List<RespBulkString> keys) throws IOException {
        int i = 0;
        for (RespBulkString key : keys) {
            if (pager.remove(key.getContent())) {
                i++;
            }
        }
        return i;
    }

    private RespBulkString nullBulk() {
        return RespBulkString.nullBulkString();
    }

    private RespError error(String msg) {
        return RespError.withUTF8(msg);
    }

    private boolean support(RespBulkString cmd) {
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);
        return Objects.equals(cmdStr, PUT_CMD)
                || Objects.equals(cmdStr, GET_CMD)
                || Objects.equals(cmdStr, DEL_CMD);
    }

    private boolean argumentsIsValid(RespBulkString cmd, RespArray req) {
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);
        switch (cmdStr) {
            case PUT_CMD:
                return req.size() == 3;
            case GET_CMD:
            case DEL_CMD:
                return req.size() >= 2;
        }
        return false;
    }
}