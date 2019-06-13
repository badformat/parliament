package io.github.parliament.paxos.client;

import io.github.parliament.paxos.acceptor.Acceptor;
import lombok.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InetPeerAcceptorsTest {
    private InetPeerAcceptors acceptors;
    private ConnectionPool connectionPool;
    private ServerSocketChannel server;
    private Thread t;
    private int poolSize = 10;
    private InetSocketAddress peer;

    @BeforeEach
    void setUp() throws IOException {
        @NonNull List<InetSocketAddress> peers = new ArrayList<>();
        peer = new InetSocketAddress("127.0.0.1", 8098);
        peers.add(peer);
        connectionPool = ConnectionPool.create(poolSize);

        acceptors = InetPeerAcceptors.builder()
                .peers(peers)
                .connectionPool(connectionPool)
                .build();

        server = ServerSocketChannel.open();
        server.bind(peer);
        server.configureBlocking(true);

        t = new Thread(() -> {
            try {
                server.accept();
            } catch (IOException e) {
                fail(e);
                return;
            }
        });
        t.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.close();
        t.interrupt();
    }

    @Test
    void create() {
        List<? extends Acceptor> a = acceptors.create(1);
        assertNotNull(a);
        assertEquals(1, a.size());
    }

    @Test
    void tooManyConnection() {
        ArrayList<SocketChannel> channels = new ArrayList<>();
        assertThrows(NoConnectionInPool.class, () -> {
            for (int i = 0; i <= connectionPool.getPoolSize(); i++) {
                channels.add(connectionPool.acquireChannel(peer));
            }
        });

        for (SocketChannel channel : channels) {
            connectionPool.releaseChannel(peer, channel, false);
        }

        assertTrue(connectionPool.getActives().get(peer).isEmpty());
        assertEquals(poolSize, connectionPool.getIdles().get(peer).size());

    }

    @Test
    void createSameRound() {
        List<? extends Acceptor> a = acceptors.create(1);
        assertThrows(IllegalStateException.class, () -> acceptors.create(1));

    }

    @Test
    void release() {
        List<? extends Acceptor> a = acceptors.create(1);
        acceptors.release(1);
    }
}