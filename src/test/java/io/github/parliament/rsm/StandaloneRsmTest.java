package io.github.parliament.rsm;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Future;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.Proposal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StandaloneRsmTest {
    private static int                        port = 28000;
    private static PaxosReplicateStateMachine machine;

    @BeforeAll
    static void beforeAll() throws Exception {
        InetSocketAddress me = new InetSocketAddress(port);
        List<InetSocketAddress> peers = new ArrayList<>();
        peers.add(me);

        ProposalPersistenceService proposalService = ProposalPersistenceService
                .builder()
                .fileService(new DefaultFileService())
                .path(Paths.get("./test", String.valueOf(port)))
                .build();
        machine = PaxosReplicateStateMachine.builder()
                .me(me)
                .peers(peers)
                .threadNo(10)
                .proposalPersistenceService(proposalService)
                .build();
        machine.start();
    }

    @AfterAll
    static void afterAll() throws IOException {
        machine.shutdown();
        Files.walk(machine.getProposalPersistenceService().getDataPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);

    }

    @Test
    void propose() throws Exception {
        byte[] p = "any value".getBytes();
        Proposal proposal = machine.propose(p);
        assertArrayEquals(p, proposal.getAgreement().get());
    }
    // TODO 文件锁
}