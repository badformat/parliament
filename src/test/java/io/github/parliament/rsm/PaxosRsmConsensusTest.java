package io.github.parliament.rsm;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.acceptor.Acceptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PaxosRsmConsensusTest {
    private static List<PaxosReplicateStateMachine> machines = new ArrayList<>();
    private static PaxosReplicateStateMachine       localMachine;

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

        for (PaxosReplicateStateMachine machine : machines) {
            machine.start();
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        for (PaxosReplicateStateMachine machine : machines) {
            machine.shutdown();
        }
        for (PaxosReplicateStateMachine machine : machines) {
            machine.shutdown();
            Files.walk(machine.getProposalPersistenceService().getDataPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    void getAcceptors() throws Exception {
        Collection<Acceptor<String>> accs = localMachine.getAcceptors(localMachine.nextRound());
        Assertions.assertEquals(5, accs.size());
    }

    @RepeatedTest(10)
    void propose() throws Exception {
        byte[] p = ("proposal " + System.currentTimeMillis()).getBytes();
        byte[] d = localMachine.propose(p).get().getAgreement();
        Assertions.assertArrayEquals(p, d);
    }

    @RepeatedTest(20)
    void proposeConcurrent() throws Exception {
        List<Future<Proposal>> futures = new ArrayList<>();
        for (PaxosReplicateStateMachine machine : machines) {
            byte[] p = ("proposal :" + System.currentTimeMillis() + "from " + machine.getMe().toString()).getBytes();
            futures.add(machine.propose(p));
        }

        for (Future<Proposal> f : futures) {
            Proposal proposal = f.get();
            for (PaxosReplicateStateMachine machine : machines) {
                while (!machine.proposal(proposal.getRound()).isPresent()) {
                    Thread.sleep(100);
                }
                Proposal mine = machine.proposal(proposal.getRound()).get();
                assertArrayEquals(proposal.getAgreement(), mine.getAgreement(), "proposal is " + proposal + ",this machine is " + mine
                );
            }
        }
    }

    @RepeatedTest(15)
    void proposeParallel() throws Exception {
        ExecutorService es = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<Proposal>> futures = new ArrayList<>();
        for (PaxosReplicateStateMachine machine : machines) {
            long r = System.currentTimeMillis();
            byte[] p = ("proposal :" + r + "from " + machine.getMe().toString()).getBytes();
            Future<Proposal> f = es.submit(() -> {
                latch.await();
                return machine.propose(p).get();
            });
            futures.add(f);
        }
        latch.countDown();

        for (Future<Proposal> f : futures) {
            Proposal proposal = f.get();
            for (PaxosReplicateStateMachine machine : machines) {
                if (!machine.proposal(proposal.getRound()).isPresent()) {
                    Thread.sleep(100);
                }
            }
        }

        for (Future<Proposal> f : futures) {
            Proposal proposal = f.get();
            for (PaxosReplicateStateMachine machine : machines) {
                assertArrayEquals(proposal.getAgreement(), machine.proposal(proposal.getRound()).get().getAgreement());
            }
        }
    }

    @Test
    void throwExceptionWhenMajorityIsDown() throws Exception {
        byte[] b = localMachine.propose("proposal".getBytes()).get().getAgreement();
        Assertions.assertArrayEquals("proposal".getBytes(), b);

        machines.get(1).shutdown();
        machines.get(2).shutdown();
        machines.get(3).shutdown();
        Thread.sleep(1000 * 2);

        int r = localMachine.nextRound();
        assertThrows(Exception.class,
                localMachine.propose("proposal".getBytes())::get);

        machines.get(1).start();
        machines.get(2).start();
        machines.get(3).start();
        r = localMachine.nextRound();

        Thread.sleep(1000 * 2);
        assertArrayEquals(("proposal" + r).getBytes(), localMachine.propose(("proposal" + r).getBytes()).get().getAgreement());
    }
}