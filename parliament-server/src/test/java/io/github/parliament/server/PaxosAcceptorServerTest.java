package io.github.parliament.server;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.github.parliament.server.PaxosAcceptorServer;

class PaxosAcceptorServerTest {
    private static PaxosAcceptorServer paxosServer;

    @BeforeAll
    static void setUp() throws Exception {
        paxosServer = new PaxosAcceptorServer();
        paxosServer.start("127.0.0.1", 18888);
    }

    @AfterAll
    static void tearDown() throws Exception {
    }

    @Test
    void testPrepare() {
        fail("Not yet implemented");
    }

    @Test
    void testAccept() {
        fail("Not yet implemented");
    }

    @Test
    void testDecided() {
        fail("Not yet implemented");
    }

    @Test
    void testStart() {
        fail("Not yet implemented");
    }

}
