package io.github.parliament.paxos;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProposerTest {
    private Proposer<String> proposer;
    private List<Acceptor<String>> acceptors = new ArrayList<Acceptor<String>>();
    private ProposalSeqNoGenerator<String> seqNoGenerator = mock(ProposalSeqNoGenerator.class);

    @BeforeEach
    void setUp() throws Exception {
        for (int n = 0; n < 5; n++) {
            acceptors.add(mock(Acceptor.class));
        }

        proposer = new Proposer<String>(acceptors, seqNoGenerator);
    }

    @Test
    void testPropose() {
        byte[] proposal = "proposal".getBytes(Charset.forName("utf-8"));
        when(seqNoGenerator.next()).thenReturn("23");
        assertEquals(proposal, proposer.propose(proposal));
        for (Acceptor<String> acceptor : acceptors) {
            verify(acceptor, times(1)).prepare(anyString());
        }
    }
}
