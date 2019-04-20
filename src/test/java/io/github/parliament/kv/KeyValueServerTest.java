package io.github.parliament.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueServerTest {
    private KeyValueServer keyValueServer;

    @BeforeAll
    void beforeAll() throws Exception {
        List<InetSocketAddress> peers = Stream.of(20000, 20001, 20002, 20003, 20004).map(InetSocketAddress::new).collect(
                Collectors.toList());

        keyValueServer = KeyValueServer.builder()
                .dir("./test")
                .kv(new InetSocketAddress(30000))
                .me(peers.get(0))
                .peers(peers)
                .build();
    }

    @AfterAll
    void afterAll() throws IOException {
        keyValueServer.shutdown();
    }

    @Test
    void put() {
        keyValueServer.put("key".getBytes(), "value".getBytes());
    }

    @Test
    void getPeers() {
        List<InetSocketAddress> peers = KeyValueServer.getPeers("127.0.0.1:50000,127.0.0.1:50001,127.0.0.1:50002");
        assertEquals(3, peers.size());
    }

    @Test
    void getMe() {
        assertEquals(new InetSocketAddress("127.0.0.1", 50002), KeyValueServer.getInetSocketAddress("127.0.0.1:50002"));
    }

}