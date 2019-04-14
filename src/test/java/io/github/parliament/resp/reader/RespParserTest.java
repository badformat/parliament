package io.github.parliament.resp.reader;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespError;
import io.github.parliament.resp.RespInteger;
import io.github.parliament.resp.RespSimpleString;
import io.github.parliament.resp.RespWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RespParserTest {
    private RespWriter          respWriter;
    private Path                path = Paths.get("./resp_test_file");
    private SeekableByteChannel writeByteChannel;
    private RespArray           a;

    @BeforeEach
    void setUp() throws Exception {
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);

        writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE);
        respWriter = RespWriter.with(writeByteChannel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Files.exists(path)) {
            Files.delete(path);
        }
        writeByteChannel.close();
    }

    @Test
    void parseEmptySimpleString() throws Exception {
        RespSimpleString es = RespSimpleString.withUTF8("");
        respWriter.append(es);
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(es, respParser.get());
    }

    @Test
    void parseSimpleString() throws Exception {
        RespSimpleString s = RespSimpleString.withUTF8("content");
        respWriter.append(s);
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(s, respParser.get());
    }

    @Test
    void parseEmptyError() throws Exception {
        RespError es = RespError.withUTF8("");
        respWriter.append(es);
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(es, respParser.get());
        channel.close();
    }

    @Test
    void parseError() throws Exception {
        RespError es = RespError.withUTF8("error msg");
        respWriter.append(es);
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(es, respParser.get());
    }

    @Test
    void parseInteger() throws Exception {
        RespInteger i = RespInteger.with(20);
        respWriter.append(i);
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(i, respParser.get());
    }

    @Test
    void parseNullBulkString() throws Exception {
        RespBulkString bs = RespBulkString.nullBulkString();
        respWriter.append(bs);
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(bs, respParser.get());
    }

    @Test
    void parseBulkString() throws Exception {
        RespBulkString bs = RespBulkString.with("测试".getBytes());
        respWriter.append(bs);
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(bs, respParser.get());
    }

    @Test
    void parseEmptyArray() throws Exception {
        respWriter.append(RespArray.empty());
        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(RespArray.empty(), respParser.get());
    }

    @Test
    void parseArray() throws Exception {
        RespSimpleString s = RespSimpleString.withUTF8("simple string");
        RespInteger i = RespInteger.with(2);
        RespBulkString bs = RespBulkString.with("bulk string".getBytes());
        a = RespArray.with(s, i, bs);
        respWriter.append(a);

        SeekableByteChannel channel = Files.newByteChannel(path);
        RespParser respParser = RespParser.create(channel);
        Assertions.assertEquals(a, respParser.get());
    }
}