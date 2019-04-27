package io.github.parliament.resp;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ByteBufTest {
    private String lorem =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua"
                    + ". Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis"
                    + " aute "
                    + "irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint "
                    + "occaecat "
                    + "cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    private byte[] bytes = lorem.getBytes();

    @Test
    void allocate() {
        ByteBuf buf = ByteBuf.allocate(20);
        assertEquals(20, buf.capacity());
    }

    @Test
    void writeByteBuffer() {
        ByteBuf buf = ByteBuf.allocate(16);

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        buf.writeBytes(bb);
        assertTrue(bytes.length < buf.capacity());

        for (byte c : bytes) {
            assertEquals(c, buf.readByte());
        }

        assertFalse(buf.isReadable());
    }

    @Test
    void readByte() {
        ByteBuf buf = ByteBuf.allocate(1024);
        buf.writeBytes(ByteBuffer.wrap(bytes));

        assertEquals(0, buf.getReaderIndex());
        assertEquals(bytes.length, buf.readableBytes());
        assertEquals(bytes.length, buf.getWriterIndex());

        buf.readByte();
        assertEquals(1, buf.getReaderIndex());
    }

    @Test
    void readBytes() {
        ByteBuf buf = ByteBuf.allocate(bytes.length);
        buf.writeBytes(ByteBuffer.wrap(bytes));

        byte[] dst = new byte[5];
        buf.readBytes(dst);

        assertArrayEquals("Lorem".getBytes(), dst);
        assertEquals(bytes.length - 5, buf.readableBytes());
    }

    @Test
    void indexOf() {
        ByteBuf buf = ByteBuf.allocate(16);
        buf.writeBytes("abcd\r\nabcd".getBytes());

        assertEquals(4, buf.indexOf(0, buf.getWriterIndex(), (byte) '\r'));
        assertEquals(-1, buf.indexOf(0, buf.getWriterIndex(), (byte) 'x'));
    }
}