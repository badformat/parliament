package io.github.parliament.paxos.rmi;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.parliament.server.PaxosServer;

class PaxosServerTest {
    private static PaxosServer paxosServer;

    @BeforeAll
    static void setUp() throws Exception {
        paxosServer = new PaxosServer();
    }

    @AfterEach
    void tearDown() throws Exception {
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
