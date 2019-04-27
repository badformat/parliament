package io.github.parliament.kv;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespSimpleString;
import io.github.parliament.rsm.PaxosReplicateStateMachine;
import io.github.parliament.rsm.ProposalPersistenceService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PaxosKeyValueEngineTest {
    private static List<PaxosReplicateStateMachine> machines = new ArrayList<>();
    private static PaxosReplicateStateMachine       localMachine;
    private static PaxosReplicateStateMachine       competitor;
    private static PaxosKeyValueEngine              keyValueEngine;

    @BeforeAll
    static void beforeAll() throws Exception {
        List<InetSocketAddress> peers = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            InetSocketAddress a = new InetSocketAddress("127.0.0.1", 18000 + i);
            peers.add(a);
        }

        for (InetSocketAddress me : peers) {
            ProposalPersistenceService proposalService = ProposalPersistenceService
                    .builder()
                    .fileService(new DefaultFileService())
                    .path(Paths.get("./test", "" + me.getPort()))
                    .build();
            PaxosReplicateStateMachine machine = PaxosReplicateStateMachine.builder()
                    .me(me)
                    .peers(peers)
                    .threadNo(10)
                    .proposalPersistenceService(proposalService)
                    .build();
            machines.add(machine);
        }

        localMachine = machines.get(0);
        competitor = machines.get(2);

        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }

        keyValueEngine = PaxosKeyValueEngine.builder().rsm(localMachine).build();
    }

    @AfterAll
    static void afterAll() throws IOException {
        for (PaxosReplicateStateMachine machine : machines) {
            machine.shutdown();
            Files.walk(machine.getProposalPersistenceService().getDataPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    void execute() throws Exception {
        RespArray request = request("put", "key", "value'");
        Future<RespData> resp = keyValueEngine.execute(request);
        assertEquals(RespSimpleString.withUTF8("ok"), resp.get());
    }

    @Test
    void executeAfterAnotherConsensus() throws Exception {
        String key = "239949595";
        String value = "239gihieghi";

        competitor.propose(request("put", key, value).toBytes()).get();
        Thread.sleep(1000);

        RespBulkString getResp = (RespBulkString) keyValueEngine.execute(request("get", key)).get();
        assertArrayEquals(RespBulkString.with(value.getBytes()).getContent(), getResp.getContent());
    }

    private RespArray request(String cmd, String... args) {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        return RespArray.with(datas);
    }
}