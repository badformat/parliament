package io.github.parliament.rsm;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.acceptor.Acceptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

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
        byte[] d = localMachine.propose(p).getAgreement().get();
        Assertions.assertArrayEquals(p, d);
    }

    @RepeatedTest(20)
    void proposeConcurrent() throws Exception {
        List<Proposal> futures = new ArrayList<>();
        for (PaxosReplicateStateMachine machine : machines) {
            byte[] p = ("proposal :" + System.currentTimeMillis() + "from " + machine.getMe().toString()).getBytes();
            futures.add(machine.propose(p));
        }

        for (Proposal proposal : futures) {
            for (PaxosReplicateStateMachine machine : machines) {
                while (!machine.proposal(proposal.getRound()).isPresent()) {
                    Thread.sleep(100);
                }
                byte[] mine = machine.proposal(proposal.getRound()).get();
                assertArrayEquals(proposal.getAgreement().get(), mine, "proposal is " + proposal + ",this machine is " + mine);
            }
        }
    }

    @Test
    void nextRound() throws Exception {
        int limit = 30;
        Set<Integer> s = new ConcurrentSkipListSet<>();
        Stream.iterate(0, (i) -> i + 1).limit(limit).parallel().peek((i) -> {
            try {
                s.add(localMachine.nextRound());
            } catch (Exception e) {
                fail(e);
            }
        }).count();
        assertEquals(30, s.size());
    }

    @Test
    void oneMachineProposeParallel() throws Exception {
        int limit = 5;
        Set<Proposal> s = Collections.synchronizedSet(new HashSet<>());

        Stream.iterate(0, (i) -> i + 1).limit(limit).parallel().peek((i) -> {
            byte[] p = ("proposal_" + i).getBytes();
            byte[] a = null;
            try {
                Proposal proposal = localMachine.propose(p);
                a = proposal.getAgreement().get();
                s.add(proposal);
            } catch (Exception e) {
                fail(e);
            }
            assertArrayEquals(p, a);
        }).count();

        assertEquals(limit, s.size());
    }

    @RepeatedTest(10)
    void proposeParallel() throws Exception {
        ExecutorService es = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<Proposal>> futures = new ArrayList<>();
        for (PaxosReplicateStateMachine machine : machines) {
            long r = System.currentTimeMillis();
            byte[] p = ("proposal :" + r + "from " + machine.getMe().toString()).getBytes();
            Future<Proposal> f = es.submit(() -> {
                latch.await();
                return machine.propose(p);
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
                assertArrayEquals(proposal.getAgreement().get(), machine.proposal(proposal.getRound()).get());
            }
        }
    }

    @Test
    void throwExceptionWhenMajorityIsDown() throws Exception {
        byte[] b = localMachine.propose("proposal".getBytes()).getAgreement().get();
        Assertions.assertArrayEquals("proposal".getBytes(), b);

        machines.get(1).shutdown();
        machines.get(2).shutdown();
        machines.get(3).shutdown();
        Thread.sleep(1000 * 2);

        int r = localMachine.nextRound();
        assertThrows(Exception.class,
                localMachine.propose("proposal".getBytes()).getAgreement()::get);

        machines.get(1).start();
        machines.get(2).start();
        machines.get(3).start();
        r = localMachine.nextRound();

        Thread.sleep(1000 * 2);
        assertArrayEquals(("proposal" + r).getBytes(), localMachine.propose(("proposal" + r).getBytes()).getAgreement().get());
    }
}