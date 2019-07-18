package io.github.parliament.paxos.server;

import io.github.parliament.MockPersistence;
import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.TimestampSequence;
import io.github.parliament.paxos.client.ConnectionPool;
import io.github.parliament.paxos.client.InetLeaner;
import io.github.parliament.paxos.client.InetPeerAcceptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class PaxosServerTest {
    private static List<PaxosServer> servers;
    private static List<InetSocketAddress> addresses;
    private static List<Paxos> paxosList = new ArrayList<>();
    private static Paxos me;
    private static Paxos other;
    private static byte[] content = "content".getBytes();
    private static volatile AtomicInteger round = new AtomicInteger();
    private static ConnectionPool pool = ConnectionPool.create(1000);

    @BeforeAll
    static void beforeAll() throws IOException {
        addresses = Stream.iterate(9000, (i) -> i + 1).limit(4)
                .map(i -> new InetSocketAddress("127.0.0.1", i))
                .collect(Collectors.toList());

        servers = addresses.stream().map(address -> {
            try {
                ArrayList<InetSocketAddress> peers = new ArrayList<>(addresses);
                peers.remove(address);
                InetPeerAcceptors acceptors = InetPeerAcceptors.builder().connectionPool(pool).peers(peers).build();
                ExecutorService executorService = Executors.newCachedThreadPool();
                InetLeaner leaner = InetLeaner.create(pool, peers);
                Paxos paxos = Paxos.builder()
                        .peerAcceptors(acceptors)
                        .learner(leaner)
                        .executorService(executorService)
                        .persistence(new MockPersistence())
                        .sequence(new TimestampSequence())
                        .build();
                paxosList.add(paxos);
                return PaxosServer.builder()
                        .me(address)
                        .paxos(paxos)
                        .build();
            } catch (IOException | ExecutionException e) {
                fail(e);
                return null;
            }
        }).collect(Collectors.toList());

        me = paxosList.get(0);
        other = paxosList.get(1);

        for (PaxosServer server : servers) {
            server.start();
        }
    }

    @AfterAll
    static void afterAll() throws IOException {
        pool.nuke();
        for (PaxosServer server : servers) {
            server.shutdown();
        }
    }

    @Test
    void coordinate() throws InterruptedException, ExecutionException, TimeoutException {
        int i = round.getAndIncrement();
        me.coordinate(i, content);
        assertArrayEquals(content, me.instance(i).get());
    }

    @RepeatedTest(10)
    void concurrentCoordinate() {
        List<Integer> rounds = Stream.iterate(1, (i) -> i + 1)
                .limit(40)
                .map(i -> round.getAndIncrement()).collect(Collectors.toList());

        rounds.stream().parallel().forEach(r -> {
            try {
                byte[] c = ("content" + r).getBytes();
                me.coordinate(r, c);
            } catch (ExecutionException e) {
                fail("failed at " + r, e);
            }
        });

        rounds.stream().parallel().forEach(r -> {
            try {
                byte[] actual = me.instance(r).get(3, TimeUnit.SECONDS);
                assertArrayEquals(("content" + r).getBytes(), actual,
                        "actual: " + new String(actual) + ",expected:content" + r);
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
                fail(e);
            }
        });
    }

    @RepeatedTest(10)
    void coordinateByTurn() {
        Stream.iterate(1, i -> i + 1).limit(30).parallel().forEach(i -> {
            try {
                int r = round.getAndIncrement();
                me.coordinate(r, ("my content" + i).getBytes());
                other.coordinate(r, ("other content" + i).getBytes());
                byte[] value = me.instance(r).get();
                if (!Arrays.equals(value, ("my content" + i).getBytes())) {
                    assertArrayEquals(value, ("other content" + i).getBytes(), "fail at " + i + ".content:" +
                            new String(value));
                }
            } catch (ExecutionException | InterruptedException e) {
                fail(e);
            }
        });
    }

    @Test
    void forget() {
        concurrentCoordinate();
        servers.stream().parallel().forEach(server -> {
            try {
                server.getPaxos().done(round.get());
            } catch (IOException | ExecutionException e) {
                fail(e);
            }
        });

        servers.stream().parallel().forEach(server -> {
            try {
                server.getPaxos().forget(round.get());
            } catch (IOException | ExecutionException e) {
                fail(e);
            }
            assertEquals(round.get(), server.getPaxos().done());
            assertEquals(round.get(), server.getPaxos().min());
        });
    }
}