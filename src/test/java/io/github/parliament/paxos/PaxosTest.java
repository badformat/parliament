package io.github.parliament.paxos;

import io.github.parliament.MockPersistence;
import io.github.parliament.Persistence;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.paxos.client.PeerAcceptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

class PaxosTest {
    private Paxos paxos;
    private byte[] value = "content".getBytes();
    private Persistence persistence = new MockPersistence();
    private PeerAcceptors peerAcceptors;

    @BeforeEach
    void setUp() throws IOException {
        List<Acceptor> acceptors = new ArrayList<>();
        acceptors.add(new LocalAcceptor(1) {
            @Override
            public void decide(byte[] agreement) {

            }
        });
        peerAcceptors = mock(PeerAcceptors.class);
        when(peerAcceptors.create(anyInt())).thenAnswer((ctx) -> acceptors);
        doNothing().when(peerAcceptors).release(anyInt());
        paxos = Paxos.builder()
                .executorService(Executors.newFixedThreadPool(10))
                .sequence(new TimestampSequence())
                .peerAcceptors(peerAcceptors)
                .persistence(persistence)
                .build();
    }

    @Test
    void coordinate() throws InterruptedException, ExecutionException, TimeoutException {
        paxos.coordinate(1, value);
        Future<byte[]> future = paxos.instance(1);
        assertNotNull(future);
        assertArrayEquals(value, future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void asyncInstance() throws InterruptedException, ExecutionException {
        Future<byte[]> future = paxos.instance(10);
        new Thread(() -> {
            try {
                paxos.coordinate(10, value);
            } catch (ExecutionException e) {
                fail(e);
            }
        }).start();
        assertArrayEquals(value, future.get());
        assertEquals(10, paxos.max());
    }

    @Test
    void instanceOfDone() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        persistence.put(ByteBuffer.allocate(4).putInt(1).array(), value);
        assertArrayEquals(value, paxos.instance(1).get(1, TimeUnit.SECONDS));
    }

    @Test
    void max() throws Exception {
        paxos.create(4).decide(value);
        assertEquals(4, paxos.max());
        assertEquals(4, ByteBuffer.wrap(persistence.get("max".getBytes())).getInt());
    }

    @Test
    void forget() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        when(peerAcceptors.done()).thenReturn(5);
        paxos.coordinate(5, value);
        paxos.instance(5).get(1, TimeUnit.SECONDS);
        paxos.done(5);
        paxos.forget(5);
        assertEquals(5, paxos.min());
        assertEquals(5, paxos.done());
    }

    @Test
    void done() throws IOException {
        paxos.done(4);
        assertEquals(4, paxos.done());
        assertEquals(4, ByteBuffer.wrap(persistence.get("done".getBytes())).getInt());
    }
}
