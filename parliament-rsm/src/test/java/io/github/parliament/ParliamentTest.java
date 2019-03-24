package io.github.parliament;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.paxos.proposer.Sequence;
import io.github.parliament.paxos.proposer.TimestampSequence;

class ParliamentTest {
    private Parliament<String> parliament;
    private ParliamentConf<String> config;
    @Mock
    private AcceptorFactory<String> acceptorFactory;
    private Sequence<String> sequence = new TimestampSequence();
    private Map<Long, List<Acceptor<String>>> acceptorsMap = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(acceptorFactory.createPeersForRound(anyLong())).then(new Answer<List<Acceptor<String>>>() {

            @Override
            public List<Acceptor<String>> answer(InvocationOnMock invocation) throws Throwable {
                long round = invocation.getArgument(0);
                return makeAcceptors(round);
            }
        });

        when(acceptorFactory.makeLocalForRound(anyLong())).then(new Answer<Acceptor<String>>() {
            @Override
            public Acceptor<String> answer(InvocationOnMock invocation) throws Throwable {
                long round = invocation.getArgument(0);
                return makeAcceptors(round).get(0);
            }
        });

        config = ParliamentConf.<String>builder().acceptorManager(acceptorFactory).dataDir("./parliament")
                .proposalPersistenceService(
                        new ProposalFilePersistence(Paths.get("./parliament"), new DefaultFileService()))
                .sequence(new TimestampSequence()).build();

        parliament = new Parliament<>(config);
        parliament.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        parliament.shutdown();

        Path path = Paths.get(config.getDataDir());
        if (Files.isDirectory(path)) {
            Files.walk(path, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    void createDataDir() throws Exception {
        Path path = Paths.get(config.getDataDir());
        assertTrue(Files.isDirectory(path));
    }

    @Test
    void createSeqFile() throws Exception {
        Path path = Paths.get(config.getDataDir(), "round");
        assertTrue(Files.exists(path));
    }

    @Test
    void initRoundIsZero() throws Exception {
        assertEquals(0L, parliament.round());
    }

    @Test
    void seqIncreasedAfterProposalDecided() throws Exception {

        for (long i = 0; i < 10; i++) {
            Proposal proposal = parliament.propose(("agreement " + i + " to reach").getBytes()).get();
            assertEquals(i, proposal.getRound());
        }
    }

    @Test
    void persistentProposal() throws Exception {
        List<Proposal> ps = new ArrayList<>();

        for (long i = 0; i < 1000; i++) {
            byte[] content = ("agreement " + i + " to reach").getBytes();

            Proposal proposal = parliament.propose(content).get();
            ps.add(proposal);
        }

        for (Proposal p : ps) {
            Optional<Proposal> regain = parliament.propoal(p.getRound());
            assertTrue(regain.isPresent());
            assertArrayEquals(p.getContent(), regain.get().getContent());
        }
    }

    @Test
    void persistentProposalConcurrent() throws Exception {
        List<Proposal> ps = new ArrayList<>();
        List<Thread> ts = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        for (long i = 0; i < 300; i++) {
            byte[] content = ("agreement " + i + " to reach").getBytes();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        latch.await();
                        Proposal proposal = parliament.propose(content).get();
                        ps.add(proposal);
                    } catch (Exception e) {
                        fail("异常", e);
                    }
                }
            });
            ts.add(t);
            t.start();
        }

        latch.countDown();

        for (Thread t : ts) {
            t.join(1000);
        }

        for (Proposal p : ps) {
            Optional<Proposal> regain = parliament.propoal(p.getRound());
            assertTrue(regain.isPresent());
            assertArrayEquals(p.getContent(), regain.get().getContent());
        }
    }

    @Test
    void testPrepare() throws Exception {
        byte[] v = "proposal content".getBytes();
        Proposal proposal = parliament.propose(v).get();
        String n = sequence.next();
        Prepare<String> prepare = parliament.prepare(proposal.getRound(), n);
        assertEquals(n, prepare.getN());
        assertTrue(prepare.getNa().compareTo(n) < 0);
        assertArrayEquals(v, prepare.getVa());
    }

    @Test
    void testPrepareForDifferentProposal() throws Exception {
        byte[] v1 = "proposal content".getBytes();
        Proposal l1 = parliament.propose(v1).get();

        byte[] v2 = "another proposal content".getBytes();
        Proposal l2 = parliament.propose(v2).get();

        String n = sequence.next();
        Prepare<String> p1 = parliament.prepare(l1.getRound(), n);
        assertArrayEquals(v1, p1.getVa());

        n = sequence.next();
        Prepare<String> p2 = parliament.prepare(l2.getRound(), n);
        assertArrayEquals(v2, p2.getVa());
    }

    private List<Acceptor<String>> makeAcceptors(long round) {
        if (!acceptorsMap.containsKey(round)) {
            List<Acceptor<String>> acceptors = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Acceptor<String> accepotr = new LocalAcceptor<>();
                acceptors.add(accepotr);
            }
            acceptorsMap.put(round, acceptors);
        }
        return acceptorsMap.get(round);
    }
}
