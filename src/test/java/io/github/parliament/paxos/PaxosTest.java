package io.github.parliament.paxos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class PaxosTest {
    private Paxos<String> paxos;

    @BeforeEach
    void setUp() throws Exception {
        paxos = new PaxosSimple();
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testPropose() throws Exception {
        byte[] value = "content".getBytes();
        assertArrayEquals(value, paxos.propose(1, value).get().getAgreement());
    }

}
