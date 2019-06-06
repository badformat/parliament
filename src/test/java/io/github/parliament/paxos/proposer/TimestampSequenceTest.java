package io.github.parliament.paxos.proposer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.github.parliament.paxos.TimestampSequence;

class TimestampSequenceTest {

    private static TimestampSequence seq;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        seq = new TimestampSequence();
    }

    @Test
    void testLength() {
        assertTrue(("123456" + "2").compareTo("123456" + "11") > 0);
        assertTrue(("123456" + "410").compareTo("123456" + "42") < 0);
    }

    @Test
    void testNext() {
        String pre = seq.next();
        for (int i = 0; i < 2000; i++) {
            String n = seq.next();
            assertTrue(n.compareTo(pre) > 0, n + " " + pre);
            pre = n;
        }
    }

}
