package io.github.parliament.paxos;

import com.google.common.base.Strings;
import io.github.parliament.MockPersistence;
import io.github.parliament.Persistence;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.paxos.client.InetLearner;
import io.github.parliament.paxos.client.PeerAcceptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

class PaxosTest {
    private Paxos paxos;
    private byte[] value = "content".getBytes();
    private Persistence persistence = new MockPersistence();
    private PeerAcceptors peerAcceptors;
    private InetLearner leaner;

    @BeforeEach
    void setUp() throws IOException, ExecutionException {
        List<Acceptor> acceptors = new ArrayList<>();
        acceptors.add(new LocalAcceptor(1) {
            @Override
            public void decide(byte[] agreement) {

            }

            @Override
            public void persistence() throws IOException, ExecutionException {
                persistence.put((round + "np").getBytes(), getNp().getBytes());
                if (!Strings.isNullOrEmpty(getNa())) {
                    persistence.put((round + "na").getBytes(), getNa().getBytes());
                }
                if (getVa() != null) {
                    persistence.put((round + "va").getBytes(), getVa());
                }
            }
        });
        peerAcceptors = mock(PeerAcceptors.class);
        when(peerAcceptors.create(anyInt())).thenAnswer((ctx) -> acceptors);
        doNothing().when(peerAcceptors).release(anyInt());

        leaner = mock(InetLearner.class);
        paxos = Paxos.builder()
                .executorService(Executors.newFixedThreadPool(10))
                .sequence(new TimestampSequence())
                .peerAcceptors(peerAcceptors)
                .learner(leaner)
                .persistence(persistence)
                .build();
    }

    @Test
    void coordinate() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        paxos.coordinate(1, value);
        Future<byte[]> future = paxos.instance(1);
        assertNotNull(future);
        assertArrayEquals(value, future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void asyncInstance() throws InterruptedException, ExecutionException, IOException {
        Future<byte[]> future = paxos.instance(10);
        new Thread(() -> {
            try {
                paxos.coordinate(10, value);
            } catch (ExecutionException | IOException e) {
                fail(e);
            }
        }).start();
        assertArrayEquals(value, future.get());
        assertEquals(10, paxos.max());
    }

    @Test
    void instanceOfDone() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        persistence.put((1 + "agreement").getBytes(), value);
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
        when(leaner.done()).thenReturn(5);
        paxos.coordinate(5, value);
        paxos.instance(5).get(1, TimeUnit.SECONDS);
        paxos.done(5);
        paxos.forget(5);
        assertEquals(5, paxos.min());
        assertEquals(5, paxos.done());
    }

    @Test
    void done() throws IOException, ExecutionException {
        paxos.done(4);
        assertEquals(4, paxos.done());
        assertEquals(4, ByteBuffer.wrap(persistence.get("done".getBytes())).getInt());
    }

    @Test
    void persistenceAcceptor() throws IOException, ExecutionException {
        Paxos.LocalAcceptorWithPersistence acceptor= paxos.new LocalAcceptorWithPersistence(100, "np", "na", "va".getBytes());
        paxos.persistenceAcceptor(100, acceptor);
        Optional<LocalAcceptor> acceptor2 = paxos.regainAcceptor(100);
        assertTrue(acceptor2.isPresent());
        assertEquals(acceptor, acceptor2.get());
    }

    @Test
    void deleteAcceptor() throws IOException, ExecutionException {
        Paxos.LocalAcceptorWithPersistence acceptor= paxos.new LocalAcceptorWithPersistence(100, "np", "na", "va".getBytes());
        paxos.persistenceAcceptor(100, acceptor);
        paxos.deleteAcceptor(100);
        assertFalse(paxos.regainAcceptor(100).isPresent());
    }
}
