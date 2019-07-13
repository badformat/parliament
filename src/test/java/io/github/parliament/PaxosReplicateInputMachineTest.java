package io.github.parliament;

import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.TimestampSequence;
import io.github.parliament.paxos.client.ConnectionPool;
import io.github.parliament.paxos.client.InetLeaner;
import io.github.parliament.paxos.client.InetPeerAcceptors;
import io.github.parliament.paxos.server.PaxosServer;
import lombok.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaxosReplicateInputMachineTest {
    private static List<InetSocketAddress> addresses;
    private static List<PaxosServer> paxosServers = new ArrayList<>();
    private static List<Paxos> paxoses = new ArrayList<>();
    private static ReplicateStateMachine me;
    private static ReplicateStateMachine other;
    private static StateTransfer transfer1;
    private static StateTransfer transfer2;
    private static int instanceNo = 20;
    private static int peersNo = 6;
    private static ConnectionPool pool = ConnectionPool.create(1000);

    @BeforeAll
    static void beforeAll() throws Exception {
        addresses = Stream.iterate(9000, (i) -> i + 1).limit(peersNo)
                .map(i -> new InetSocketAddress("127.0.0.1", i))
                .collect(Collectors.toList());

        addresses.forEach(me -> {
            try {
                ArrayList<InetSocketAddress> peers = new ArrayList<>(addresses);
                peers.remove(me);
                InetPeerAcceptors acceptors = InetPeerAcceptors.builder().peers(peers).connectionPool(pool).build();
                ExecutorService executorService = Executors.newCachedThreadPool();
                @NonNull InetLeaner learner = InetLeaner.create(pool, peers);
                Paxos paxos = Paxos.builder()
                        .peerAcceptors(acceptors)
                        .learner(learner)
                        .executorService(executorService)
                        .persistence(new MockPersistence())
                        .sequence(new TimestampSequence())
                        .build();
                paxoses.add(paxos);
                PaxosServer paxosServer = PaxosServer.builder()
                        .me(me)
                        .paxos(paxos)
                        .build();
                paxosServer.start();
                paxosServers.add(paxosServer);
            } catch (Exception e) {
                fail(e);
            }
        });

        me = ReplicateStateMachine.builder()
                .persistence(new MockPersistence())
                .sequence(new IntegerSequence())
                .coordinator(paxoses.get(0))
                .build();
        transfer1 = mock(StateTransfer.class);
        me.setStateTransfer(transfer1);

        other = ReplicateStateMachine.builder()
                .persistence(new MockPersistence())
                .sequence(new IntegerSequence())
                .coordinator(paxoses.get(1))
                .build();
        transfer2 = mock(StateTransfer.class);
        other.setStateTransfer(transfer2);
        mockOutput();
        me.start(transfer1, Executors.newSingleThreadExecutor());
        other.start(transfer2, Executors.newSingleThreadExecutor());
    }

    @AfterAll
    static void afterAll() throws IOException {
        pool.nuke();
        for (PaxosServer server : paxosServers) {
            server.shutdown();
        }
    }

    @RepeatedTest(10)
    void concurrentSubmit() throws Exception {
        Optional<Integer> failed = Stream.iterate(1, i -> i + 1).limit(instanceNo).parallel().map(i -> {
            Input input = me.newState(("content" + i).getBytes());
            try {
                Output output = me.submit(input).get(3, TimeUnit.SECONDS);
                assertArrayEquals(output.getContent(), (("output of " + new String(input.getContent())).getBytes()));
                return 0;
            } catch (ExecutionException | IOException | InterruptedException | TimeoutException e) {
                return 1;
            }
        }).reduce((i, j) -> i + j);

        assertTrue(failed.isPresent());

        assertTrue(failed.get() < instanceNo / 2);
        keepUp();
    }

    @RepeatedTest(10)
    void conflictSubmit() throws InterruptedException {
        List<CompletableFuture<Output>> myOutputs = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Output>> otherOutputs = Collections.synchronizedList(new ArrayList<>());

        Optional<Integer> failed = Stream.iterate(1, i -> i + 1).limit(instanceNo).map(i -> {
            try {
                myOutputs.add(me.submit(me.newState(("my content" + i).getBytes())));
                otherOutputs.add(other.submit(other.newState(("other content" + i).getBytes())));
                return 0;
            } catch (IOException | ExecutionException e) {
                return 1;
            }
        }).reduce((i, j) -> i + j);

        assertTrue(failed.isPresent());

        assertTrue(failed.get() < instanceNo / 2);

        failed = myOutputs.stream().map(f -> {
            try {
                Output output1 = f.get(3, TimeUnit.SECONDS);
                Output output2 = other.getTransforms().get(output1.getId()).get(3, TimeUnit.SECONDS);
                assertEquals(output1, output2);
                return 0;
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                return 1;
            }
        }).reduce((i, j) -> i + j);

        assertTrue(failed.isPresent());

        assertTrue(failed.get() < instanceNo / 2);
        keepUp();
    }

    @RepeatedTest(10)
    void submitByTurns() throws Exception {
        Stream.iterate(1, i -> i + 1).limit(instanceNo).parallel().forEach(i -> {
            Input input = me.newState(("my content" + i).getBytes());
            try {
                me.submit(input).get(1, TimeUnit.SECONDS);
            } catch (ExecutionException | IOException | InterruptedException | TimeoutException e) {
                fail(e);
            }
        });
        Stream.iterate(1, i -> i + 1).limit(instanceNo).parallel().forEach(i -> {
            Input input = other.newState(("other content" + i).getBytes());
            try {
                other.submit(input).get(1, TimeUnit.SECONDS);
            } catch (ExecutionException | IOException | InterruptedException | TimeoutException e) {
                fail(e);
            }
        });
        keepUp();
    }

    @RepeatedTest(10)
    void concurrentConflictSubmit() throws InterruptedException {
        List<CompletableFuture<Output>> myOutputs = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Output>> otherOutputs = Collections.synchronizedList(new ArrayList<>());

        Stream.iterate(1, i -> i + 1).limit(instanceNo).parallel().forEach(i -> {
            try {
                myOutputs.add(me.submit(me.newState(("my content" + i).getBytes())));
                otherOutputs.add(other.submit(other.newState(("other content" + i).getBytes())));
            } catch (IOException | ExecutionException e) {
                fail(e);
            }
        });

        Optional<Integer> failed = myOutputs.stream().parallel().map(f -> {
            try {
                Output output1 = f.get(3, TimeUnit.SECONDS);
                Output output2 = other.getTransforms().get(output1.getId()).get(3, TimeUnit.SECONDS);
                assertEquals(output1.getId(), output2.getId());
                assertEquals(new String(output1.getContent()), new String(output2.getContent()));
                assertArrayEquals(output1.getUuid(), output2.getUuid());
                return 0;
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                return 1;
            }
        }).reduce((i, j) -> i + j);

        assertTrue(failed.isPresent());

        assertTrue(failed.get() < instanceNo / 2);
        keepUp();
    }

    @Test
    void recovery() throws Exception {
        keepUp();
        other.stop();

        Stream.iterate(1, i -> i + 1).limit(instanceNo).parallel().forEach(i -> {
            Input input = me.newState(("my content" + i).getBytes());
            try {
                me.submit(input).get(1, TimeUnit.SECONDS);
            } catch (ExecutionException | IOException | InterruptedException | TimeoutException e) {
                fail(e);
            }
        });

        other.start(transfer2, Executors.newSingleThreadExecutor());

        concurrentSubmit();
        concurrentConflictSubmit();
    }

    private static void keepUp() throws InterruptedException {
        int time = 0;
        while (me.done() != other.done() || me.current() != other.current()) {
            Thread.sleep(10);
            time++;
            if (time > 500) {
                fail("so long to keep up with each other.me.done()=" + me.done() + ".other.done()=" + other.done()
                        + ".me.current()=" + me.current() + ".other.current()=" + other.current()
                        + ".me.max()=" + me.max() + ".other.max()=" + other.max());
            }
        }

        System.out.println("keep up success.me.done()=" + me.done() + ".other.done()=" + other.done()
                + ".me.current()=" + me.current() + ".other.current()=" + other.current()
                + ".me.max()=" + me.max() + ".other.max()=" + other.max());
    }

    private static void mockOutput() throws Exception {
        when(transfer1.transform(any())).thenAnswer((ctx) -> {
            Input input = (Input) ctx.getArguments()[0];
            return Output.builder()
                    .content(("output of " + new String(input.getContent())).getBytes())
                    .uuid(input.getUuid())
                    .id(input.getId())
                    .build();
        });

        when(transfer2.transform(any())).thenAnswer((ctx) -> {
            Input input = (Input) ctx.getArguments()[0];
            return Output.builder()
                    .content(("output of " + new String(input.getContent())).getBytes())
                    .uuid(input.getUuid())
                    .id(input.getId())
                    .build();
        });
    }
}