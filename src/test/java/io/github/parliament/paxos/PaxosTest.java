package io.github.parliament.paxos;

import io.github.parliament.MockPersistence;
import io.github.parliament.Persistence;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.paxos.client.PeerAcceptors;
import lombok.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class PaxosTest {
    private Paxos paxos;
    private byte[] value = "content".getBytes();
    private Persistence persistence = new MockPersistence();

    @BeforeEach
    void setUp() {
        @NonNull PeerAcceptors factory = new PeerAcceptors() {
            @Override
            public List<? extends Acceptor> create(int round) {
                List<Acceptor> acceptors = new ArrayList<>();
                acceptors.add(new LocalAcceptor(round) {
                    @Override
                    public void decide(byte[] agreement) {

                    }

                    @Override
                    public void failed(String error) {

                    }
                });
                return acceptors;
            }

            @Override
            public void release(int round) {

            }
        };

        paxos = Paxos.builder()
                .executorService(Executors.newFixedThreadPool(10))
                .sequence(new TimestampSequence())
                .peerAcceptors(factory)
                .persistence(persistence)
                .build();
    }

    @AfterEach
    void tearDown() {

    }

    @Test
    void coordinate() throws InterruptedException, ExecutionException, TimeoutException {
        paxos.coordinate(1, value);
        Future<byte[]> future = paxos.instance(1);
        assertNotNull(future);
        assertArrayEquals(value, future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void asyncInstance() throws InterruptedException, ExecutionException, TimeoutException {
        Future<byte[]> future = paxos.instance(10);
        new Thread(() -> paxos.coordinate(10, value)).start();
        assertArrayEquals(value, future.get());
        assertEquals(10, paxos.max());
    }

    @Test
    void instanceOfDone() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        persistence.put(ByteBuffer.allocate(4).putInt(1).array(), value);
        assertArrayEquals(value, paxos.instance(1).get(1, TimeUnit.SECONDS));
    }
}
