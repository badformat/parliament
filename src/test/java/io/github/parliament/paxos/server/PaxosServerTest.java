package io.github.parliament.paxos.server;

import io.github.parliament.MockPersistence;
import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.TimestampSequence;
import io.github.parliament.paxos.client.InetPeerAcceptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
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
    private static byte[] content = "content".getBytes();
    private static volatile AtomicInteger round = new AtomicInteger();

    @BeforeAll
    static void beforeAll() throws IOException {
        addresses = Stream.iterate(9000, (i) -> i + 1).limit(4)
                .map(i -> new InetSocketAddress("127.0.0.1", i))
                .collect(Collectors.toList());

        servers = addresses.stream().map(address -> {
            try {
                ArrayList<InetSocketAddress> peers = new ArrayList<>(addresses);
                peers.remove(address);
                InetPeerAcceptors acceptors = InetPeerAcceptors.builder().peers(peers).pmc(100).build();
                ExecutorService executorService = new ThreadPoolExecutor(
                        3,
                        30,
                        10,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(100));
                Paxos paxos = Paxos.builder()
                        .peerAcceptors(acceptors)
                        .executorService(executorService)
                        .persistence(new MockPersistence())
                        .sequence(new TimestampSequence())
                        .build();
                paxosList.add(paxos);
                return PaxosServer.builder()
                        .me(address)
                        .paxos(paxos)
                        .build();
            } catch (IOException e) {
                fail(e);
                return null;
            }
        }).collect(Collectors.toList());

        me = paxosList.get(0);

        for (PaxosServer server : servers) {
            server.start();
        }
    }

    @AfterAll
    static void afterAll() throws IOException {
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

    @Test
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

    @Test
    void forget() {
        concurrentCoordinate();
        servers.stream().parallel().forEach(server -> {
            try {
                server.getPaxos().done(round.get());
            } catch (IOException e) {
                fail(e);
            }
        });

        servers.stream().parallel().forEach(server -> {
            try {
                server.getPaxos().forget(round.get());
            } catch (IOException e) {
                fail(e);
            }
            assertEquals(round.get(), server.getPaxos().done());
            assertEquals(round.get() + 1, server.getPaxos().min());
        });
    }
}