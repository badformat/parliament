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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespInteger;
import io.github.parliament.rsm.PaxosReplicateStateMachine;
import io.github.parliament.rsm.ProposalPersistenceService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class PaxosKeyValueEngineTest {
    private static List<PaxosReplicateStateMachine> machines = new ArrayList<>();
    private static PaxosReplicateStateMachine       me;
    private static PaxosReplicateStateMachine       competitor;
    private static PaxosKeyValueEngine              kv;
    private static PaxosKeyValueEngine              kv2;

    @BeforeAll
    static void beforeAll() throws Exception {
        List<InetSocketAddress> peers = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            InetSocketAddress a = new InetSocketAddress("127.0.0.1", 18000 + i);
            peers.add(a);
        }

        for (InetSocketAddress me : peers) {
            ProposalPersistenceService persistenceSerivce = ProposalPersistenceService
                    .builder()
                    .fileService(new DefaultFileService())
                    .path(Paths.get("./test", "" + me.getPort()))
                    .build();
            PaxosReplicateStateMachine machine = PaxosReplicateStateMachine.builder()
                    .me(me)
                    .peers(peers)
                    .threadNo(10)
                    .proposalPersistenceService(persistenceSerivce)
                    .build();
            machines.add(machine);
        }

        me = machines.get(0);
        competitor = machines.get(2);

        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }

        kv = PaxosKeyValueEngine.builder().rsm(me).path(me.getProposalPersistenceService().getDataPath().resolve("db"))
                .build();
        kv2 = PaxosKeyValueEngine.builder().rsm(competitor).path(competitor.getProposalPersistenceService().getDataPath().resolve("db"))
                .build();
        kv.start();
        kv2.start();
    }

    @AfterAll
    static void afterAll() throws IOException {
        kv.shutdown();
        kv2.shutdown();
        for (PaxosReplicateStateMachine machine : machines) {
            machine.shutdown();
            Files.walk(machine.getProposalPersistenceService().getDataPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    void put() throws Exception {
        RespArray request = request("put", "key", "value'");
        Future<RespData> resp = kv.execute(request);
        assertEquals(RespInteger.with(1), resp.get());
    }

    @Test
    void getAfterAnotherConsensus() throws Exception {
        String key = "239949595";
        String value = "239gihieghi";

        kv2.execute(request("put", key, value)).get();
        Thread.sleep(200);
        RespBulkString resp = (RespBulkString) kv.execute(request("get", key)).get();
        assertArrayEquals(value.getBytes(), resp.getContent());
    }

    @Test
    void del() throws Exception {
        String key = "239949595";
        String value = "239gihieghi";

        RespInteger ok = (RespInteger) kv.execute(request("put", key, value)).get();
        assertEquals(RespInteger.with(1), ok);

        RespBulkString resp = (RespBulkString) kv.execute(request("get", key)).get();
        assertEquals(RespBulkString.with(value.getBytes()), resp);

        assertEquals(RespInteger.with(1), kv.execute(request("del", key)).get());
    }

    @Test
    void putIfAbsent() throws Exception {
        String key = "233";
        String value = "239ghxxf";

        RespInteger ok = (RespInteger) kv.execute(request("put", key, value)).get();
        assertEquals(RespInteger.with(1), ok);

        String newValue = "233333";
        RespInteger resp = (RespInteger) kv.execute(request("putIfAbsent", key, newValue)).get();
        assertEquals(RespInteger.with(0), resp);

        RespBulkString getResp = (RespBulkString) kv.execute(request("get", key)).get();
        assertEquals(RespBulkString.with(value.getBytes()), getResp);
    }

    // parallel requests
    @Test
    void putDifferentKvsInParallel() {
        int limit = 10;
        Map<Integer, Future<RespData>> ps = new ConcurrentHashMap<>();
        assertEquals(limit, Stream.iterate(0, (i) -> i + 1).limit(limit).parallel().peek((i) -> {
            String key = String.valueOf(i);
            try {
                ps.put(i, kv.execute(request("put", key, key)));
            } catch (Exception e) {
                fail(e);
            }
        }).peek((i) -> {
            try {
                RespData resp = ps.get(i).get();
                assertEquals(RespInteger.with(1), resp);
                RespData value = kv.execute(request("get", String.valueOf(i))).get();
                assertEquals(RespBulkString.with(String.valueOf(i).getBytes()), value);
            } catch (Exception e) {
                fail(e);
            }
        }).count());
    }

    /**
     * 保证实例间memtable一致
     */
    @Test
    void memtableConsistency() {
        int limit = 20;
        Map<Integer, Future<RespData>> r1 = new ConcurrentHashMap<>();
        Map<Integer, Future<RespData>> r2 = new ConcurrentHashMap<>();

        List<Integer> noList = Stream.iterate(0, (i) -> i + 1).limit(limit).parallel().peek((i) -> {
            try {
                String key = String.valueOf(i);

                String value = String.valueOf(ThreadLocalRandom.current().nextInt());
                r1.put(i, kv.execute(request("put", key, value + ",kv id:" + 1)));

                value = String.valueOf(ThreadLocalRandom.current().nextInt());
                r2.put(i, kv2.execute(request("put", key, value + ",kv id:" + 2)));
            } catch (Exception e) {
                fail(e);
            }
        }).peek((i) -> {
            try {
                r1.get(i).get();
                r2.get(i).get();
            } catch (InterruptedException | ExecutionException e) {
                fail(e);
            }
        }).collect(Collectors.toList());

        noList.forEach((i) -> {
            String key = String.valueOf(i);
            try {
                assertEquals(kv.execute(request("get", key)).get(), kv2.execute(request("get", key)).get());
            } catch (Exception e) {
                fail(e);
            }
        });

        assertEquals(kv.getMemtable(), kv2.getMemtable());
    }

    //TODO cursor persistence
    // TODO wal log
    // TODO simple persistence key value store.
    // TODO dirty file

    private RespArray request(String cmd, String... args) {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        return RespArray.with(datas);
    }
}