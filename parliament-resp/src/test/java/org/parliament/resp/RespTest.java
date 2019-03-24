package org.parliament.resp;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import com.google.common.primitives.Bytes;

class RespTest {
    private RespWriter respWriter;
    private RespReader respReader;
    private Path path = Paths.get("./resp_file");
    private SeekableByteChannel writeByteChannel;
    private SeekableByteChannel readByteChannel;
    private Charset charset = Charset.forName("utf-8");
    private int capacity = 1;

    @BeforeEach
    void setUp() throws Exception {
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);

        writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE);
        respWriter = RespWriter.with(writeByteChannel);

        readByteChannel = Files.newByteChannel(path);
        respReader = RespReader.with(readByteChannel, capacity);
        capacity += capacity * 10;
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Files.exists(path)) {
            Files.delete(path);
        }
        writeByteChannel.close();
        readByteChannel.close();
    }

    @RepeatedTest(10)
    void appendSimpleString() throws Exception {
        RespData rs = new RespSimpleString("内容", charset);
        respWriter.append(rs);

        RespSimpleString ss = respReader.getSimpleString(charset).get();

        assertEquals(rs, ss);
    }

    @RepeatedTest(10)
    void readEmptySimpleString() throws Exception {
        RespData rs = new RespSimpleString("", charset);
        respWriter.append(rs);
        RespSimpleString ss = respReader.getSimpleString(charset).get();
        assertEquals(rs, ss);
    }

    @RepeatedTest(10)
    void appendMultipleSimpleString() throws Exception {

        for (int i = 0; i < 100; i++) {
            RespData rs = new RespSimpleString("内容" + i, charset);
            respWriter.append(rs);
        }

        for (int i = 0; i < 100; i++) {
            RespData rs = new RespSimpleString("内容" + i, charset);
            RespSimpleString ss = respReader.getSimpleString(charset).get();
            assertEquals(rs, ss);
        }

    }

    @RepeatedTest(10)
    void readIllegalSimpleString() throws Exception {
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            seekableByteChannel.write(ByteBuffer.wrap("内容".getBytes()));
        }

        assertFalse(respReader.getSimpleString(charset).isPresent());
    }

    @RepeatedTest(10)
    void readNotEndSimpleString() throws Exception {
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            seekableByteChannel.write(ByteBuffer.wrap("+内容".getBytes()));
        }

        assertThrows(IllegalStateException.class, () -> respReader.getSimpleString(charset).isPresent());
    }

    @RepeatedTest(10)
    void readNotEndSimpleString2() throws Exception {
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            seekableByteChannel.write(ByteBuffer.wrap("+内容\r".getBytes()));
        }

        assertThrows(IllegalStateException.class, () -> respReader.getSimpleString(charset).isPresent());
    }

    @RepeatedTest(10)
    void appendRespError() throws Exception {
        for (int i = 0; i < 100; i++) {
            RespData rs = new RespError("内容" + i, charset);
            respWriter.append(rs);
        }

        for (int i = 0; i < 100; i++) {
            RespData rs = new RespError("内容" + i, charset);
            RespError rs2 = respReader.getError(charset).get();
            assertEquals(rs, rs2);
        }
    }

    @RepeatedTest(10)
    void appendSimpleStringAndError() throws Exception {
        RespData e = new RespError("error 内容", charset);
        respWriter.append(e);

        RespData s = new RespSimpleString("simple content", charset);
        respWriter.append(s);

        assertFalse(respReader.getSimpleString(charset).isPresent());
        assertEquals(e, respReader.getError(charset).get());
        assertEquals(s, respReader.getSimpleString(charset).get());
    }

    @RepeatedTest(5)
    void readInteger() throws Exception {
        RespData i = new RespInteger("2333", charset);

        respWriter.append(i);

        assertEquals(Integer.valueOf("2333"), respReader.getInteger(charset).get().getN());
    }

    @RepeatedTest(5)
    void readIllegalInteger() throws Exception {
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            seekableByteChannel.write(ByteBuffer.wrap(":233a\r\n".getBytes()));
        }

        assertThrows(NumberFormatException.class, () -> {
            respReader.getInteger(charset);
        });
    }

    @RepeatedTest(5)
    void readBulkString() throws Exception {
        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            seekableByteChannel.write(ByteBuffer.wrap("$6\r\nfoobar\r\n".getBytes()));
        }

        RespBulkString bulkString = respReader.getBulkString().get();
        assertArrayEquals("foobar".getBytes(), bulkString.getContent());
    }

    @Test
    void readIntegerAndBulkString() throws Exception {
        RespInteger round = new RespInteger(1);
        byte[] content = "内容".getBytes();
        RespBulkString bulk = new RespBulkString(content);

        respWriter.append(round, bulk);

        assertEquals(Integer.valueOf(1), respReader.getInteger(charset).get().getN());
        assertArrayEquals(content, respReader.getBulkString().get().getContent());
    }

    @Test
    void builString() throws Exception {
        RespBulkString s = new RespBulkString("测试".getBytes());
        respWriter.append(s);

        RespBulkString bulkString = respReader.getBulkString().get();
        assertArrayEquals("测试".getBytes(), bulkString.getContent());
    }

    @RepeatedTest(5)
    void readBulkString2() throws Exception {
        byte[] content = "测试\r\n测试".getBytes();
        byte[] head = ("$" + content.length + "\r\n").getBytes();

        byte[] both = Bytes.concat(head, content, "\r\n".getBytes());

        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            seekableByteChannel.write(ByteBuffer.wrap(both));
        }

        RespBulkString bulkString = respReader.getBulkString().get();
        assertArrayEquals(content, bulkString.getContent());
    }

    @RepeatedTest(5)
    void readEmptyBulkString() throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap("$0\r\n\r\n".getBytes()));
        }
        RespBulkString bulkString = respReader.getBulkString().get();
        assertArrayEquals("".getBytes(), bulkString.getContent());
    }

    @RepeatedTest(5)
    void readNullBulkString() throws Exception {
        RespBulkString bulkString = new RespBulkString(null);
        assertNull(bulkString.getContent());
        assertEquals(-1, bulkString.getLength());
        assertArrayEquals("$-1\r\n".getBytes(), bulkString.toBytes());

        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
        }
        bulkString = respReader.getBulkString().get();
        assertNull(bulkString.getContent());
        assertEquals(-1, bulkString.getLength());
    }

    @RepeatedTest(5)
    void readNegativeLengthBulkString() throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap("$-2\r\n".getBytes()));
        }
        assertThrows(IllegalStateException.class, () -> respReader.getBulkString());
    }

    @Test
    void simpleArray() throws Exception {
        byte[] bytes = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".getBytes();
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
        }
        RespArray array = respReader.getArray(charset).get();
        assertArrayEquals(bytes, array.toBytes());
    }

    @Test
    void zeroLengthArray() throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap("*0\r\n".getBytes()));
        }
        RespArray array = respReader.getArray(charset).get();
        assertEquals(0, array.getDatas().size());
    }

    @Test
    void arrayWithSameData() throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            List<RespData> datas = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                RespSimpleString s = new RespSimpleString("测试测试" + i, charset);
                datas.add(s);
            }
            respWriter.append(RespArray.with(datas));
        }
        RespArray array = respReader.getArray(charset).get();
        assertEquals(100, array.getDatas().size());
        for (int i = 0; i < 100; i++) {
            RespSimpleString s = new RespSimpleString("测试测试" + i, charset);
            assertEquals(s, array.getDatas().get(i));
        }
    }

    @Test
    void array() throws Exception {
        RespSimpleString s = new RespSimpleString("测试测试", charset);
        RespError e = new RespError("错误", charset);
        RespBulkString bs = new RespBulkString("bulk字符".getBytes());
        RespArray a = RespArray.with(s, e, bs);

        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            respWriter.append(a);
        }

        RespArray array = respReader.getArray(charset).get();
        assertEquals(s, array.get(0));
        assertEquals(e, array.get(1));
        assertEquals(bs, array.get(2));
    }

    @RepeatedTest(10)
    void arrayWithArray() throws Exception {
        RespSimpleString s = new RespSimpleString("测试测试", charset);
        RespError e = new RespError("错误", charset);
        RespBulkString bs = new RespBulkString("bulk字符".getBytes());
        RespSimpleString es = new RespSimpleString("", charset);

        RespArray a = RespArray.with(s, e, es, bs);

        RespSimpleString s2 = new RespSimpleString("测试测试测试测试", charset);
        RespError e2 = new RespError("错误", charset);
        RespBulkString bs2 = new RespBulkString("bulk字符2".getBytes());

        RespArray a2 = RespArray.with(s2, a, e2, bs2);

        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            respWriter.append(a2);
        }
        RespArray array = respReader.getArray(charset).get();
        assertEquals(4, array.size());
        assertEquals(s2, array.get(0));
        assertEquals(a, array.get(1));
        assertEquals(e2, array.get(2));
        assertEquals(bs2, array.get(3));
    }
}
