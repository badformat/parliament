package io.github.parliament.resp;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * jdk自带的{@link ByteBuffer}功能很初始，解析协议非常复杂，网路开发一般都需要自己再封装一些buffer实现。
 * 该类的读写位置是独立的，不用考虑flip和rewind。
 * 再复杂一点，可以把ByteBuf的构造和回收自己托管起来，避免频繁GC。
 * @author zy
 */
public class ByteBuf {
    // 底层字节数组
    private byte[] buf;
    // 当前读位置
    @Getter
    private int    readerIndex;
    // 当前写位置
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

    /**
     * @return 当前还可以写入的大小
     */
    public int writableBytes() {
        return buf.length - writerIndex;
    }

    /**
     * 向buf写入数据
     * @param bb 数据源
     * @return 本对象
     */
    public ByteBuf writeBytes(ByteBuffer bb) {
        ensureWritable(bb.remaining());
        while (bb.hasRemaining()) {
            writeByte0(bb.get());
        }
        return this;
    }

    /**
     * 向buf写入数据
     * @param bytes 数据源
     * @return 本对象
     */
    public ByteBuf writeBytes(byte[] bytes) {
        ensureWritable(bytes.length);
        for (byte b : bytes) {
            writeByte0(b);
        }
        return this;
    }

    /**
     * 写一个字节
     * @param b 字节
     * @return 本对象
     */
    public ByteBuf writeByte(byte b) {
        ensureWritable(1);
        writeByte0(b);
        return this;
    }

    private void writeByte0(byte b) {
        buf[writerIndex] = b;
        writerIndex++;
    }

    /**
     * 是否有数据未消费，可读取
     * @return true 有，false 没有
     */
    public boolean isReadable() {
        return readerIndex < writerIndex;
    }

    /**
     * 读一个字节
     * @return 字节
     * @throws IllegalStateException 没有可读数据
     */
    public byte readByte() {
        Preconditions.checkState(readerIndex < writerIndex);
        return buf[readerIndex++];
    }

    /**
     * 查询多少数据可读
     * @return 可读取数据的大小
     */
    public int readableBytes() {
        return writerIndex - readerIndex;
    }

    /**
     * 读数据
     * @param bytes 读到的数据的存放数组
     * @return 本对象
     * @throws IndexOutOfBoundsException 没有足够数据填充数组
     */
    public ByteBuf readBytes(byte[] bytes) {
        if (bytes.length > readableBytes()) {
            throw new IndexOutOfBoundsException();
        }

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = readByte();
        }

        return this;
    }

    /**
     * 使用绝对索引读取字节
     * @param index 索引
     * @return 字节值
     * @throws IndexOutOfBoundsException 索引越界
     */
    public byte getByte(int index) {
        return buf[index];
    }

    /**
     * 查找某字节值的索引
     * @param fromIndex 从该索引开始
     * @param toIndex 到该索引结束
     * @param value 查找的字节值
     * @return 该值的第一个索引值
     */
    public int indexOf(int fromIndex, int toIndex, byte value) {
        for (int i = fromIndex; i < toIndex; i++) {
            if (buf[i] == value) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 缓存最大容量
     * @return 容量
     */
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