package io.github.parliament.kv;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    private static final Logger                              logger            = LoggerFactory.getLogger(PaxosKeyValueEngine.class);
    private static final String                              PUT_CMD           = "put";
    private static final String                              GET_CMD           = "get";
    private static final String                              DEL_CMD           = "del";
    private static final String                              PUT_IF_ABSENT_CMD = "putIfAbsent";
    private              PaxosReplicateStateMachine          rsm;
    @Getter(AccessLevel.PACKAGE)
    private final        Map<RespBulkString, RespBulkString> memtable          = new ConcurrentHashMap<>();
    private volatile     int                                 cursor            = 0;
    private volatile     BlockingQueue<RequestWork>          works             = new ArrayBlockingQueue<>(200);
    private              Thread                              worker;
    @Getter(AccessLevel.PACKAGE)
    private              Path                                path;

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
    PaxosKeyValueEngine(@NonNull Path path, @NonNull PaxosReplicateStateMachine rsm) {
        this.path = path;
        this.rsm = rsm;
    }

    public void start() throws Exception {
        worker = new Thread(this);
        worker.start();
    }

    public void shutdown() {
        worker.interrupt();
    }

    @Override
    public void run() {
        for (; ; ) {
            try {
                RequestWork work = works.poll(10, TimeUnit.SECONDS);
                if (work == null) {
                    continue;
                }

                byte[] request = work.getRequest();
                Proposal proposal = rsm.propose(request);

                CompletableFuture<RespData> f = work.getFuture();

                try {
                    catchUp(proposal.getRound());
                } catch (Exception e) {
                    logger.error("catch up failed.", e);
                    f.complete(error(e));
                }
                int round = proposal.getRound();

                byte[] value = proposal.getAgreement().get();
                RespData resp;
                if (Arrays.equals(value, request)) {
                    resp = execute(round, value);
                } else {
                    execute(round, value);
                    //System.out.println("request: "+ new String(request) + ", consensus: "+new String(value));
                    resp = RespError.withUTF8("共识冲突");
                }

                f.complete(resp);
            } catch (InterruptedException e) {
                logger.error("thread interrupted.", e);
            } catch (Exception e) {
                logger.error("thread error.", e);
            }
        }
    }

    @Override
    public Future<RespData> execute(final RespArray request) {
        CompletableFuture<RespData> f = new CompletableFuture<>();
        works.add(RequestWork.builder().future(f).request(request.toBytes()).build());
        return f;
    }

    private void catchUp(int round) throws Exception {
        while (cursor < round) {
            Optional<byte[]> proprosal = rsm.proposal(cursor);
            if (!proprosal.isPresent()) {
                rsm.pull(cursor).get();
                proprosal = rsm.proposal(cursor);
            }

            if (!proprosal.isPresent()) {
                rsm.propose(RespArray.with().toBytes());
                throw new PreviousRequestNotFoundExcpetion("proposal for round " + round + "not exists,propose a empty array.");
            }

            execute(cursor, proprosal.get());
        }
    }

    private RespData execute(int round, byte[] request) throws UnknownKeyValueCommand {
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
                memtable.put(key, value);
                ret = RespInteger.with(1);
                break;
            case GET_CMD:
                key = consensusReq.get(1);
                ret = memtable.containsKey(key) ? memtable.get(key) : nullBulk();
                break;
            case DEL_CMD:
                List<RespBulkString> keys = consensusReq.getDatas();
                ret = RespInteger.with(del(keys.subList(1, keys.size())));
                break;
            case PUT_IF_ABSENT_CMD:
                key = consensusReq.get(1);
                value = consensusReq.get(2);
                ret = memtable.putIfAbsent(key, value) == null ? RespInteger.with(1) : RespInteger.with(0);
                break;
            default:
                throw new UnknownKeyValueCommand("Unknown key value service: " + cmdStr);
        }
        cursor = round + 1;
        return ret;
    }

    private int del(List<RespBulkString> keys) {
        int i = 0;
        for (RespData key : keys) {
            if (memtable.remove(key) != null) {
                i++;
            }
        }
        return i;
    }

    private RespBulkString nullBulk() {
        return RespBulkString.nullBulkString();
    }

    private RespError error(Exception e) {
        return RespError.withUTF8(e.getMessage());
    }
}