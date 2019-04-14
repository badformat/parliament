package io.github.parliament.rsm;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.rsm.StateMachineEvent.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PaxosRsmRoundTest {
    private List<PaxosReplicateStateMachine> machines = new ArrayList<>();
    private PaxosReplicateStateMachine       localMachine;

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
        }

        localMachine = machines.get(0);
    }

    @AfterEach
    void afterEach() throws Exception {
        for (PaxosReplicateStateMachine peer : machines) {
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
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
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
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
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
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
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
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }
        for (int i = 0; i < 10; i++) {
            int r = localMachine.nextRound();
            localMachine.propose(r, ("proposal of round " + r).getBytes()).get();
        }
        assertEquals(9, localMachine.maxRound());
    }

    @Test
    void learn() throws Exception {
        List<PaxosReplicateStateMachine> actives = machines.subList(0, machines.size() - 2);
        PaxosReplicateStateMachine defer = machines.get(machines.size() - 1);
        for (PaxosReplicateStateMachine machine : actives) {
            machine.start();
        }

        List<Future<Proposal>> proposals = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int r = localMachine.nextRound();
            proposals.add(localMachine.propose(r, ("proposal of round " + r).getBytes()));
        }

        Map<Integer, Proposal> events = new HashMap<>();
        for (Future<Proposal> proposal : proposals) {
            events.put(proposal.get().getRound(), proposal.get());
        }

        defer.start();
        defer.sync();

        for (int round : events.keySet()) {
            StateMachineEvent event = defer.event(round);
            assertArrayEquals(events.get(round).getAgreement(), event.getAgreement());
        }
    }
}