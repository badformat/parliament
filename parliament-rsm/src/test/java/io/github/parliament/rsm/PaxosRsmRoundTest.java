package io.github.parliament.rsm;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.files.FileService;
import io.github.parliament.rsm.StateMachineEvent.Status;
import io.github.parliament.server.sync.PaxosSyncServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PaxosRsmRoundTest {
    private List<PaxosSyncServer>            servers  = new ArrayList<>();
    private List<PaxosReplicateStateMachine> machines = new ArrayList<>();
    private PaxosReplicateStateMachine       localMachine;

    private List<InetSocketAddress> peersAddresses = new ArrayList<>();
    private FileService             fileService    = new DefaultFileService();

    @BeforeEach
    void beforeEach() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        List<InetSocketAddress> peers = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            InetSocketAddress a = new InetSocketAddress("127.0.0.1", 18000 + i);
            peers.add(a);
        }

        for (InetSocketAddress me : peers) {
            RoundPersistenceService roundService = RoundPersistenceService
                    .builder()
                    .fileService(new DefaultFileService())
                    .path(Paths.get("./test", "" + me.getPort()))
                    .build();

            PaxosReplicateStateMachine machine = PaxosReplicateStateMachine.builder()
                    .me(me)
                    .peers(peers)
                    .executorService(executorService)
                    .roundPersistenceService(roundService)
                    .build();
            machines.add(machine);

            PaxosSyncServer server = PaxosSyncServer.builder()
                    .acceptorFactory(machine)
                    .executorService(executorService)
                    .me(me)
                    .build();
            servers.add(server);
        }

        localMachine = machines.get(0);
    }

    @AfterEach
    void afterEach() throws Exception {
        for (PaxosSyncServer peer : servers) {
            peer.shutdown();
        }
        for (PaxosReplicateStateMachine machine : machines) {
            Files.walk(machine.getRoundPersistenceService().getDataPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        Thread.sleep(200);
    }

    @Test
    void newRound() throws Exception {
        for (PaxosSyncServer server : servers) {
            server.start();
        }
        int r = localMachine.nextRound();
        byte[] p = localMachine.propose(r, "proposal".getBytes()).get().getAgreement();

        for (PaxosReplicateStateMachine machine : machines) {
            assertEquals(r, machine.event(r).getRound());
            assertEquals(Status.decided, machine.event(r).getStatus());
            assertArrayEquals(p, machine.event(r).getAgreement());
        }
    }

    @Test
    void reProposeRound() throws Exception {
        for (PaxosSyncServer server : servers) {
            server.start();
        }
        int r = localMachine.nextRound();
        byte[] p = localMachine.propose(r, "proposal".getBytes()).get().getAgreement();

        assertArrayEquals(p, localMachine.propose(r, "proposal 2".getBytes()).get().getAgreement());
    }

    @Test
    void unknownEvent() throws Exception {
        int r = localMachine.nextRound();
        assertEquals(Status.unknown, localMachine.event(r).getStatus());
    }

    @Test
    void min() throws Exception {
        assertEquals(0, localMachine.minRound());
    }

    @Test
    void forgetRounds() throws Exception {
        for (PaxosSyncServer server : servers) {
            server.start();
        }
        for (int i = 0; i < 10; i++) {
            int r = localMachine.nextRound();
            localMachine.propose(r, ("proposal of round " + r).getBytes()).get();
        }
        localMachine.forgetRoundsTo(5);
        assertEquals(5, localMachine.minRound());
    }

    @Test
    void max() throws Exception {
        for (PaxosSyncServer server : servers) {
            server.start();
        }
        for (int i = 0; i < 10; i++) {
            int r = localMachine.nextRound();
            localMachine.propose(r, ("proposal of round " + r).getBytes()).get();
        }
        assertEquals(9, localMachine.maxRound());
    }
}