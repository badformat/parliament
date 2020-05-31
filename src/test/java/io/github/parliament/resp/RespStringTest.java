package io.github.parliament.resp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RespStringTest {

    @Test
    void toBytes() {
        byte[] s = RespSimpleString.withUTF8("OK").toBytes();
        assertArrayEquals("+OK\r\n".getBytes(), s);
    }
}