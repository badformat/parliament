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
    private InetPeerAcceptors factory;
    private ServerSocketChannel server;
    private Thread t;
    private int pmc = 10;
    private InetSocketAddress peer;

    @BeforeEach
    void setUp() throws IOException {
        @NonNull List<InetSocketAddress> peers = new ArrayList<>();
        peer = new InetSocketAddress("127.0.0.1", 8098);
        peers.add(peer);
        factory = InetPeerAcceptors.builder()
                .peers(peers)
                .pmc(10)
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
        List<? extends Acceptor> a = factory.create(1);
        assertNotNull(a);
        assertEquals(1, a.size());
    }

    @Test
    void tooManyConnection() {
        ArrayList<SocketChannel> channels = new ArrayList<>();
        assertThrows(NoConnectionInPool.class, () -> {
            for (int i = 0; i <= pmc; i++) {
                channels.add(factory.acquireChannel(peer));
            }
        });

        for (SocketChannel channel : channels) {
            factory.releaseChannel(peer, channel, false);
        }

        assertTrue(factory.getBusyChannels().get(peer).isEmpty());
        assertEquals(pmc, factory.getIdleChannels().get(peer).size());

    }

    @Test
    void createSameRound() {
        List<? extends Acceptor> a = factory.create(1);
        assertThrows(IllegalStateException.class, () -> factory.create(1));

    }

    @Test
    void release() {
        List<? extends Acceptor> a = factory.create(1);
        factory.release(1);
    }
}