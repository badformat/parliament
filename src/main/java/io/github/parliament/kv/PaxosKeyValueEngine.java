package io.github.parliament.kv;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import io.github.parliament.paxos.Proposal;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespDecoder;
import io.github.parliament.resp.RespError;
import io.github.parliament.resp.RespSimpleString;
import io.github.parliament.rsm.PaxosReplicateStateMachine;
import lombok.Builder;
import lombok.NonNull;

/**
 *
 * @author zy
 */
public class PaxosKeyValueEngine implements KeyValueEngine {
    public static final String                              PUT_CMD = "put";
    public static final String                              GET_CMD = "get";
    public static final String                              DEL_CMD = "del";
    private             PaxosReplicateStateMachine          rsm;
    private             Map<RespBulkString, RespBulkString> cache   = new ConcurrentHashMap<>();
    private             int                                 cursor  = 0;

    @Builder
    PaxosKeyValueEngine(@NonNull PaxosReplicateStateMachine rsm) {
        this.rsm = rsm;
    }

    @Override
    public Future<RespData> execute(RespArray request) throws Exception {
        catchUp();

        return CompletableFuture.supplyAsync(() -> {
            try {
                Future<Proposal> pf = rsm.propose(request.toBytes());
                Proposal proposal = pf.get();
                if (Arrays.equals(proposal.getAgreement(), request.toBytes())) {
                    return execute(proposal);
                } else {
                    execute(proposal);
                    return RespError.withUTF8("共识冲突");
                }
            } catch (Exception e) {
                return error(e);
            }
        });
    }

    private void catchUp() throws Exception {
        int maxRound = rsm.maxRound();
        while (cursor <= maxRound) {
            Optional<Proposal> proprosal = rsm.proposal(cursor);
            if (!proprosal.isPresent()) {
                rsm.pull(cursor).get();
                proprosal = rsm.proposal(cursor);
            } // TODO

            execute(proprosal.get());
        }
    }

    private RespData execute(Proposal proposal) throws UnknownKeyValueCommand {
        RespDecoder decoder = new RespDecoder();
        decoder.decode(proposal.getAgreement());
        RespArray consensusReq = decoder.get();
        RespBulkString cmd = consensusReq.get(0);
        String cmdStr = new String(cmd.getContent(), StandardCharsets.UTF_8);
        switch (cmdStr) {
            case PUT_CMD:
                RespBulkString key = consensusReq.get(1);
                RespBulkString value = consensusReq.get(2);
                cache.put(key, value);
                cursor = proposal.getRound() + 1;
                return ok();
            case GET_CMD:
                key = consensusReq.get(1);
                cursor = proposal.getRound() + 1;
                return cache.containsKey(key) ? cache.get(key) : nullBulk();
            default:
                throw new UnknownKeyValueCommand("Unknown key value service: " + cmdStr);
        }
    }

    private RespBulkString nullBulk() {
        return RespBulkString.nullBulkString();
    }

    private RespSimpleString ok() {
        return RespSimpleString.withUTF8("ok");
    }

    private RespError error(Exception e) {
        return RespError.withUTF8(e.getMessage());
    }
}