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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.rsm.StateMachineEvent.Status;
import io.github.parliament.server.sync.PaxosSyncServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PaxosRsmConsensusTest {
    private static List<PaxosSyncServer>            servers  = new ArrayList<>();
    private static List<PaxosReplicateStateMachine> machines = new ArrayList<>();
    private static PaxosReplicateStateMachine       localMachine;

    @BeforeAll
    static void beforeAll() throws Exception {
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

        for (PaxosSyncServer server : servers) {
            server.start();
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        for (PaxosSyncServer paxosSyncServer : servers) {
            paxosSyncServer.shutdown();
        }
        for (PaxosReplicateStateMachine machine : machines) {
            machine.shutdown();
            Files.walk(machine.getRoundPersistenceService().getDataPath())
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
        int r = localMachine.nextRound();
        byte[] p = ("proposal " + r).getBytes();
        byte[] d = localMachine.propose(r, p).get().getAgreement();
        Assertions.assertArrayEquals(p, d);
    }

    @RepeatedTest(20)
    void proposeConcurrent() throws Exception {
        List<Future<Proposal>> futures = new ArrayList<>();
        for (PaxosReplicateStateMachine machine : machines) {
            int r = machine.nextRound();
            byte[] p = ("proposal :" + r + "from " + machine.getMe().toString()).getBytes();
            futures.add(machine.propose(r, p));
        }

        for (Future<Proposal> f : futures) {
            Proposal proposal = f.get();
            for (PaxosReplicateStateMachine machine : machines) {
                while (machine.event(proposal.getRound()).getStatus() == Status.unknown) {
                    Thread.sleep(100);
                }
                StateMachineEvent mine = machine.event(proposal.getRound());
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
            int r = machine.nextRound();
            byte[] p = ("proposal :" + r + "from " + machine.getMe().toString()).getBytes();
            Future<Proposal> f = es.submit(() -> {
                latch.await();
                return machine.propose(r, p).get();
            });
            futures.add(f);
        }
        latch.countDown();

        for (Future<Proposal> f : futures) {
            Proposal proposal = f.get();
            for (PaxosReplicateStateMachine machine : machines) {
                if (machine.event(proposal.getRound()).getStatus() == Status.unknown) {
                    Thread.sleep(100);
                }
            }
        }

        for (Future<Proposal> f : futures) {
            Proposal proposal = f.get();
            for (PaxosReplicateStateMachine machine : machines) {
                assertArrayEquals(proposal.getAgreement(), machine.event(proposal.getRound()).getAgreement());
            }
        }
    }

    @Test
    void throwExceptionWhenMajorityIsDown() throws Exception {
        int r = localMachine.nextRound();

        byte[] b = localMachine.propose(r, "proposal".getBytes()).get().getAgreement();
        Assertions.assertEquals(new String("proposal".getBytes()), new String(b));

        machines.get(1).leaveAgreementProcess();
        machines.get(2).leaveAgreementProcess();
        machines.get(3).leaveAgreementProcess();

        r = localMachine.nextRound();
        assertThrows(ExecutionException.class,
                localMachine.propose(r, "proposal".getBytes())::get);

        machines.get(1).joinAgreementProcess();
        machines.get(2).joinAgreementProcess();
        machines.get(3).joinAgreementProcess();
    }
}