package io.github.parliament;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.parliament.paxos.Acceptor;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.TimestampIpSequence;

class ParliamentTest {
    static private Parliament<String> parliament;

    static private ArrayList<Acceptor<String>> acceptors = new ArrayList<>();

    private static ParliamentConf<String> config;

    @BeforeAll
    static void setUpBeforeClass() {

        for (int i = 0; i < 4; i++) {
            @SuppressWarnings("unchecked")
            Acceptor<String> accepotr = mock(Acceptor.class);
            acceptors.add(accepotr);
        }

        config = ParliamentConf.<String>builder().acceptors(acceptors).dataDir("./parliament")
                .sequence(new TimestampIpSequence()).build();
    }

    @AfterAll
    static void tearDownAfterClass() {

    }

    @BeforeEach
    void setUp() {
        parliament = new Parliament<>(config);
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
        parliament.start();
        Path path = Paths.get(config.getDataDir());
        assertTrue(Files.isDirectory(path));
    }

    @Test
    void createSeqFile() throws Exception {
        parliament.start();
        Path path = Paths.get(config.getDataDir(), "local", "seq");
        assertTrue(Files.exists(path));
    }

    @Test
    void initSeqIsZero() throws Exception {
        parliament.start();
        assertEquals(0L, parliament.seq());
    }

    @Test
    void seqIncreasedAfterProposalDecided() throws Exception {
        parliament.start();

        for (long i = 0; i < 1000; i++) {
            Proposal proposal = parliament.propose(("agreement " + i + " to reach").getBytes());
            assertEquals(i, proposal.getRound());
        }
    }

    @Test
    void persistentProposal() throws Exception {
        parliament.start();
        List<Proposal> ps = new ArrayList<>();

        for (long i = 0; i < 1000; i++) {
            byte[] content = ("agreement " + i + " to reach").getBytes();

            Proposal proposal = parliament.propose(content);
            ps.add(proposal);
        }

        for (Proposal p : ps) {
            Optional<Proposal> regain = parliament.regainProposal(p.getRound());
            assertTrue(regain.isPresent());
            assertEquals(p, regain.get());
        }
    }

    @Test
    void persistentProposalConcurrent() throws Exception {
        parliament.start();
        List<Proposal> ps = new ArrayList<>();
        List<Thread> ts = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        for (long i = 0; i < 30; i++) {
            byte[] content = ("agreement " + i + " to reach").getBytes();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        latch.await();
                        Proposal proposal = parliament.propose(content);
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
            Optional<Proposal> regain = parliament.regainProposal(p.getRound());
            assertTrue(regain.isPresent());
            assertEquals(p, regain.get());
        }

    }

}
