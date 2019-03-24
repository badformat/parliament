package io.github.parliament.paxos;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.inject.Guice;

class PaxosTest {
    private Paxos paxos;

    @BeforeEach
    void setUp() throws Exception {
        paxos = Guice.createInjector(new PaxosTestModule()).getInstance(Paxos.class);
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testPropose() throws InterruptedException, ExecutionException {
        byte[] value = "content".getBytes();
        assertArrayEquals(value, paxos.propose(1, value).get());
    }

}
