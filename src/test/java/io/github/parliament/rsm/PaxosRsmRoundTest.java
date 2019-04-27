package io.github.parliament.rsm;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.Proposal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            ProposalPersistenceService proposalService = ProposalPersistenceService
                    .builder()
                    .fileService(new DefaultFileService())
                    .path(Paths.get("./test", "" + me.getPort()))
                    .build();

            PaxosReplicateStateMachine machine = PaxosReplicateStateMachine.builder()
                    .me(me)
                    .peers(peers)
                    .threadNo(3)
                    .proposalPersistenceService(proposalService)
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
            Files.walk(machine.getProposalPersistenceService().getDataPath())
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
        int r = 2333333;

        Proposal pf = localMachine.propose(r, "proposal".getBytes()).get();

        for (PaxosReplicateStateMachine machine : machines) {
            Optional<Proposal> proposal = machine.proposal(pf.getRound());
            assertTrue(proposal.isPresent());
            assertEquals(r, proposal.get().getRound());
            assertArrayEquals(pf.getAgreement(), proposal.get().getAgreement());
        }
    }

    @Test
    void reProposeRound() throws Exception {
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }
        int r = 23303;
        byte[] p = localMachine.propose(r, "proposal".getBytes()).get().getAgreement();

        assertArrayEquals(p, localMachine.propose(r, "proposal 2".getBytes()).get().getAgreement());
    }

    @Test
    void unknownEvent() throws Exception {
        int r = localMachine.nextRound();
        assertFalse(localMachine.proposal(r).isPresent());
    }

    @Test
    void min() throws Exception {
        assertEquals(-1, localMachine.minRound());
    }

    @Test
    void forgetRounds() throws Exception {
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }
        for (int i = 0; i < 10; i++) {
            long r = System.currentTimeMillis();
            localMachine.propose(("proposal of round " + r).getBytes()).get();
        }
        localMachine.forget(5);
        assertEquals(5, localMachine.minRound());
    }

    @Test
    void max() throws Exception {
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }
        for (int i = 0; i < 10; i++) {
            long r = System.currentTimeMillis();
            localMachine.propose(("proposal of round " + r).getBytes()).get();
        }
        assertEquals(9, localMachine.maxRound());
    }

    @Test
    void learnMax() throws Exception {
        List<PaxosReplicateStateMachine> actives = machines.subList(0, machines.size() - 1);
        PaxosReplicateStateMachine defer = machines.get(machines.size() - 1);
        for (PaxosReplicateStateMachine machine : actives) {
            machine.start();
        }

        List<Future<Proposal>> proposals = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            long r = System.currentTimeMillis();
            proposals.add(localMachine.propose(("proposal of round " + r).getBytes()));
        }

        for (Future<Proposal> proposal : proposals) {
            proposal.get();
        }

        for (int max : defer.getLearner().learnMax()) {
            assertEquals(localMachine.maxRound(), max);
        }
        for (int min : defer.getLearner().learnMin()) {
            assertEquals(localMachine.minRound(), min);
        }
    }

    @Test
    void learn() throws Exception {
        List<PaxosReplicateStateMachine> actives = machines.subList(0, machines.size() - 1);
        PaxosReplicateStateMachine defer = machines.get(machines.size() - 1);
        for (PaxosReplicateStateMachine machine : actives) {
            machine.start();
        }

        List<Future<Proposal>> proposals = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            long r = System.currentTimeMillis();
            proposals.add(localMachine.propose(("proposal of round " + r).getBytes()));
        }

        for (Future<Proposal> proposal : proposals) {
            proposal.get();
        }

        assertTrue(defer.pull().call());

        assertEquals(localMachine.maxRound(), defer.maxRound());
    }

    @Test
    void forget() throws Exception {
        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }

        List<Future<Proposal>> proposals = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            long r = System.currentTimeMillis();
            proposals.add(localMachine.propose(("proposal of round " + i).getBytes()));
        }

        for (Future<Proposal> proposal : proposals) {
            proposal.get();
        }

        localMachine.forget(9);
        assertEquals(9, localMachine.minRound());
        assertTrue(localMachine.proposal(9).isPresent());
        assertArrayEquals("proposal of round 9".getBytes(), localMachine.proposal(9).get().getAgreement());
    }
}