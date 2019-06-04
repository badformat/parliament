package io.github.parliament.paxos.proposer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.parliament.Sequence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;

class ProposerTest {
    private Proposer<String>       proposer;
    private List<Acceptor<String>> acceptors = new ArrayList<Acceptor<String>>();
    @Mock
    private Sequence<String> seqNoGenerator;
    @Mock
    private Acceptor<String>       acc1;
    @Mock
    private Acceptor<String>       acc2;
    @Mock
    private Acceptor<String>       acc3;
    @Mock
    private Acceptor<String>       acc4;
    @Mock
    private Acceptor<String>       acc5;

    private AtomicInteger ai = new AtomicInteger();
    private byte[]        proposal;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        acceptors.add(acc1);
        acceptors.add(acc2);
        acceptors.add(acc3);
        acceptors.add(acc4);
        acceptors.add(acc5);

        proposal = "proposal".getBytes();
        proposer = new Proposer<String>(acceptors, seqNoGenerator, proposal);

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
        for (Acceptor<String> acc : acceptors) {
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
        for (Acceptor<String> acceptor : acceptors) {
            verify(acceptor, times(1)).prepare(anyString());
        }
        assertTrue(proposer.isDecided());
    }

    @Test
    void prepareRejectedByMajority() throws Exception {
        String n = seqNoGenerator.next();

        Prepare<String> rejectPrepare = Prepare.reject(n);
        when(acc1.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc2.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc3.prepare(anyString())).thenReturn(rejectPrepare);

        proposer.setN(n);
        Prepare<String> prepare = Prepare.ok(n, n, proposal);

        when(acc4.prepare(anyString())).thenReturn(prepare);
        when(acc5.prepare(anyString())).thenReturn(prepare);

        assertFalse(proposer.prepare());
        assertFalse(proposer.isDecided());
    }

    @Test
    void prepareRejectedBySomeone() throws Exception {
        String n = seqNoGenerator.next();

        Prepare<String> rejectPrepare = Prepare.reject(n);

        when(acc1.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc2.prepare(anyString())).thenReturn(rejectPrepare);

        proposer.setN(n);
        Prepare<String> prepare = Prepare.ok(n, n, proposal);

        when(acc3.prepare(anyString())).thenReturn(prepare);
        when(acc4.prepare(anyString())).thenReturn(prepare);
        when(acc5.prepare(anyString())).thenReturn(prepare);

        assertTrue(proposer.prepare());
    }

    @Test
    void prepareReturnedWithAcceptedValue() throws Exception {
        String n = seqNoGenerator.next();
        Prepare<String> rejectPrepare = Prepare.reject(n);

        when(acc1.prepare(anyString())).thenReturn(rejectPrepare);
        when(acc2.prepare(anyString())).thenReturn(rejectPrepare);

        proposer.setN(n);
        Prepare<String> prepare = Prepare.ok(n, n, proposal);

        when(acc3.prepare(anyString())).thenReturn(prepare);
        when(acc4.prepare(anyString())).thenReturn(prepare);

        byte[] va = "another proposal".getBytes();
        prepare = Prepare.ok(n, seqNoGenerator.next(), va);
        when(acc5.prepare(anyString())).thenReturn(prepare);

        assertTrue(proposer.prepare());
        assertEquals(va, proposer.getAgreement());
    }

    @Test
    void acceptRejectedByMajority() throws Exception {
        String n = seqNoGenerator.next();
        Accept<String> rejectAccept = Accept.reject(n);
        when(acc1.accept(anyString(), any())).thenReturn(rejectAccept);
        when(acc2.accept(anyString(), any())).thenReturn(rejectAccept);
        when(acc3.accept(anyString(), any())).thenReturn(rejectAccept);

        Accept<String> accept = Accept.ok(n);
        when(acc4.accept(anyString(), any())).thenReturn(accept);
        when(acc5.accept(anyString(), any())).thenReturn(accept);

        proposer.setN(n);

        assertFalse(proposer.accept());
    }

    @Test
    void acceptRejectedBySomeone() throws Exception {
        String n = seqNoGenerator.next();
        Accept<String> rejectAccept = Accept.reject(n);
        when(acc1.accept(anyString(), any())).thenReturn(rejectAccept);
        when(acc2.accept(anyString(), any())).thenReturn(rejectAccept);

        Accept<String> accept = Accept.ok(n);
        when(acc3.accept(anyString(), any())).thenReturn(accept);
        when(acc4.accept(anyString(), any())).thenReturn(accept);
        when(acc5.accept(anyString(), any())).thenReturn(accept);

        proposer.setN(n);

        assertTrue(proposer.accept());
    }
}
