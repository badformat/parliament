package io.github.parliament.resp;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RespDecoderTest {
    private RespDecoder decoder;

    @BeforeEach
    void beforeEach() {
        decoder = RespDecoder.create();
    }

    @Test
    void decodeSimpleString() {
        ByteBuffer buf = ByteBuffer.wrap("+OK\r\n".getBytes());
        decoder.decode(buf);
        RespSimpleString s = decoder.get();
        assertNotNull(s);
        assertEquals("OK", s.getContent());
    }

    @Test
    void decodeSimpleString2() {
        ByteBuffer buf = ByteBuffer.wrap("+OK\r".getBytes());
        decoder.decode(buf);

        buf = ByteBuffer.wrap("\n".getBytes());
        decoder.decode(buf);

        RespSimpleString s = decoder.get();
        assertNotNull(s);
        assertEquals("OK", s.getContent());
    }

    @Test
    void decodeInteger() {
        ByteBuffer buf = ByteBuffer.wrap(":1000\r\n".getBytes());
        decoder.decode(buf);
        RespInteger i = decoder.get();
        assertEquals(1000, i.getN().intValue());
    }

    @Test
    void decodeInteger2() {
        ByteBuffer buf = ByteBuffer.wrap(":102201".getBytes());
        decoder.decode(buf);

        buf = ByteBuffer.wrap("\r\n".getBytes());
        decoder.decode(buf);

        RespInteger i = decoder.get();
        assertEquals(102201, i.getN().intValue());
    }

    @Test
    void decodeBulkString() {
        byte[] c = "$6\r\nfoobar\r\n".getBytes();
        ByteBuffer buf = ByteBuffer.wrap(c);
        decoder.decode(buf);

        RespBulkString bs = decoder.get();
        assertArrayEquals("foobar".getBytes(), bs.getContent());
    }

    @Test
    void decodeBulkString2() {
        decoder.decode("$6\r".getBytes());
        decoder.decode("\nfoobar".getBytes());
        decoder.decode("\r".getBytes());
        decoder.decode("\n".getBytes());

        RespBulkString bs = decoder.get();
        assertArrayEquals("foobar".getBytes(), bs.getContent());
    }

    @Test
    void decodeEmptyBulkString() {
        decoder.decode("$0\r\n\r\n".getBytes());
        RespBulkString bs = decoder.get();

        assertArrayEquals("".getBytes(), bs.getContent());
    }

    @Test
    void decodeNullBulkString() {
        decoder.decode("$-1\r\n".getBytes());
        RespBulkString bs = decoder.get();

        assertArrayEquals(null, bs.getContent());
    }

    @Test
    void decodeArray() {
        decoder.decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".getBytes());
        RespArray array = decoder.get();

        assertEquals(2, array.size());
    }

    @Test
    void decodeArray2() {
        decoder.decode("*2\r\n$3\r\nfoo\r\n$3\r".getBytes());
        decoder.decode("\nbar\r\n".getBytes());

        RespArray array = decoder.get();
        assertEquals(2, array.size());
    }

    @Test
    void decodeEmptyArray() {
        decoder.decode("*0\r\n".getBytes());

        RespArray array = decoder.get();
        assertEquals(0, array.size());
    }

    @Test
    void decodeNestArray() {
        RespArray nestArray = RespArray.with(RespSimpleString.withUTF8("hi"), RespBulkString.with("cookies".getBytes()),
                RespArray.with(RespSimpleString.withUTF8("nest element"), RespArray.empty(), RespInteger.with(233)), RespArray.empty());

        decoder.decode(nestArray.toByteBuffer());
        RespArray array = decoder.get();
        assertEquals(4, array.size());

        assertEquals(nestArray, array);
    }

    @Test
    void continuesDecode() {
        decodeSimpleString();
        decodeArray();
        decodeArray2();
        decodeBulkString2();
        decodeNestArray();
        decodeEmptyArray();
    }
}