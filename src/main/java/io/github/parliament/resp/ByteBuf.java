package io.github.parliament.resp;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 *
 * @author zy
 */
public class ByteBuf {
    private byte[] buf;
    @Getter
    private int    readerIndex;
    @Getter
    private int    writerIndex;

    private ByteBuf(int i) {
        buf = new byte[i];
        readerIndex = 0;
        writerIndex = 0;
    }

    public static ByteBuf allocate(int i) {
        return new ByteBuf(i);
    }

    public int writableBytes() {
        return buf.length - writerIndex;
    }

    public ByteBuf writeBytes(ByteBuffer bb) {
        ensureWritable(bb.remaining());
        while (bb.hasRemaining()) {
            writeByte0(bb.get());
        }
        return this;
    }

    public ByteBuf writeBytes(byte[] bytes) {
        ensureWritable(bytes.length);
        for (byte b : bytes) {
            writeByte0(b);
        }
        return this;
    }

    public ByteBuf writeByte(byte b) {
        ensureWritable(1);
        writeByte0(b);
        return this;
    }

    private void writeByte0(byte b) {
        buf[writerIndex] = b;
        writerIndex++;
    }

    public boolean isReadable() {
        return readerIndex < writerIndex;
    }

    public byte readByte() {
        Preconditions.checkState(readerIndex < writerIndex);
        return buf[readerIndex++];
    }

    public int readableBytes() {
        return writerIndex - readerIndex;
    }

    public ByteBuf readBytes(byte[] bytes) {
        if (bytes.length > readableBytes()) {
            throw new IndexOutOfBoundsException();
        }

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = readByte();
        }

        return this;
    }

    public byte getByte(int index) {
        return buf[index];
    }

    public int indexOf(int fromIndex, int toIndex, byte value) {
        for (int i = fromIndex; i < toIndex; i++) {
            if (buf[i] == value) {
                return i;
            }
        }
        return -1;
    }

    public int capacity() {
        return buf.length;
    }

    private void ensureWritable(int remaining) {
        if (writableBytes() < remaining) {
            capacity(buf.length * 2 + remaining);
        }
    }

    private void capacity(int i) {
        Preconditions.checkState(i > buf.length);
        buf = Arrays.copyOf(buf, i);
    }

}