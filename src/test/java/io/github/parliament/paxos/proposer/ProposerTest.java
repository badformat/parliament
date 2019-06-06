package io.github.parliament.paxos.proposer;

import io.github.parliament.Sequence;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProposerTest {
    private Proposer proposer;
    private List<Acceptor> peers = new ArrayList<>();
    private List<Acceptor> all = new ArrayList<>();
    @Mock
    private Sequence<String> seqNoGenerator;
    @Mock
    private Acceptor acc1;
    @Mock
    private Acceptor acc2;
    @Mock
    private Acceptor acc3;
    @Mock
    private Acceptor acc4;
    @Mock
    private LocalAcceptor local;

    private AtomicInteger ai = new AtomicInteger();
    private byte[] proposal;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        peers.add(acc1);
        peers.add(acc2);
        peers.add(acc3);
        peers.add(acc4);

        proposal = "proposal".getBytes();
        proposer = new Proposer(local, peers, seqNoGenerator, proposal);

        all.addAll(peers);
        all.add(local);
        when(seqNoGenerator.next()).thenAnswer((ctx) -> {
            return String.valueOf(ai.getAndIncrement());
        });
    }

    @Test
    void majority() {
        assertEquals(1, proposer.calcMajority(1));
        assertEquals(2, proposer.calcMajority(2));
        assertEquals(2, proposer.calcMajority(3));
        assertEquals(3, proposer.calcMajority(4));
        assertEquals(3, proposer.calcMajority(5));
        assertEquals(4, proposer.calcMajority(6));
        assertEquals(4, proposer.calcMajority(7));
    }

    @Test
    void propose() throws Exception {
        for (Acceptor acc : all) {
            when(acc.prepare(anyString())).thenAnswer((ctx) -> {
                String n = ctx.getArgument(0);
                return Prepare.ok(n, n, proposal);
            });

            when(acc.accept(anyString(), any())).thenAnswer((ctx) -> {
                String n = ctx.getArgument(0);
                return Accept.ok(n);
            });
        }

        proposer.propose();
        for (Acceptor acceptor : peers) {
            verify(acceptor, times(1)).prepare(anyString());
        }
        assertTrue(proposer.isDecided());
    }

    @Test
    void prepareRejectedByMajority() throws Exception {
        String n = seqNoGenerator.next();

        Prepare rejectPrepare = Prepare.reject(n);
        when(acc1.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc2.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc3.prepare(anyString())).thenReturn(rejectPrepare);

        proposer.setN(n);
        Prepare prepare = Prepare.ok(n, n, proposal);

        when(acc4.prepare(anyString())).thenReturn(prepare);
        when(local.prepare(anyString())).thenReturn(prepare);

        assertFalse(proposer.prepare());
        assertFalse(proposer.isDecided());
    }

    @Test
    void prepareRejectedBySomeone() throws Exception {
        String n = seqNoGenerator.next();

        Prepare rejectPrepare = Prepare.reject(n);

        when(acc1.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc2.prepare(anyString())).thenReturn(rejectPrepare);

        proposer.setN(n);
        Prepare prepare = Prepare.ok(n, n, proposal);

        when(acc3.prepare(anyString())).thenReturn(prepare);
        when(acc4.prepare(anyString())).thenReturn(prepare);
        when(local.prepare(anyString())).thenReturn(prepare);

        assertTrue(proposer.prepare());
    }

    @Test
    void prepareReturnedWithAcceptedValue() throws Exception {
        String n = seqNoGenerator.next();
        Prepare rejectPrepare = Prepare.reject(n);

        when(acc1.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc2.prepare(anyString())).thenReturn(rejectPrepare);

        proposer.setN(n);
        Prepare prepare = Prepare.ok(n, n, proposal);

        when(acc3.prepare(anyString())).thenReturn(prepare);
        when(acc4.prepare(anyString())).thenReturn(prepare);

        byte[] va = "another proposal".getBytes();
        prepare = Prepare.ok(n, seqNoGenerator.next(), va);
        when(local.prepare(anyString())).thenReturn(prepare);

        assertTrue(proposer.prepare());
        assertEquals(va, proposer.getAgreement());
    }

    @Test
    void acceptRejectedByMajority() throws Exception {
        String n = seqNoGenerator.next();
        Accept rejectAccept = Accept.reject(n);
        when(acc1.accept(anyString(), any())).thenReturn(rejectAccept);
        when(acc2.accept(anyString(), any())).thenReturn(rejectAccept);
        when(acc3.accept(anyString(), any())).thenReturn(rejectAccept);

        Accept accept = Accept.ok(n);
        when(acc4.accept(anyString(), any())).thenReturn(accept);
        when(local.accept(anyString(), any())).thenReturn(accept);

        proposer.setN(n);

        assertFalse(proposer.accept());
    }

    @Test
    void acceptRejectedBySomeone() throws Exception {
        String n = seqNoGenerator.next();
        Accept rejectAccept = Accept.reject(n);
        when(acc1.accept(anyString(), any())).thenReturn(rejectAccept);
        when(acc2.accept(anyString(), any())).thenReturn(rejectAccept);

        Accept accept = Accept.ok(n);
        when(acc3.accept(anyString(), any())).thenReturn(accept);
        when(acc4.accept(anyString(), any())).thenReturn(accept);
        when(local.accept(anyString(), any())).thenReturn(accept);

        proposer.setN(n);

        assertTrue(proposer.accept());
    }
}
