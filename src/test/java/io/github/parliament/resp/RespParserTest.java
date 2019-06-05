package io.github.parliament.resp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

class RespParserTest {
    private Path path = Paths.get("./resp_file");
    private RespArray a;

    @BeforeEach
    void setUp() throws IOException {
        Files.createFile(path);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

    @Test
    void parseEmptySimpleString() throws Exception {
        RespSimpleString es = RespSimpleString.withUTF8("");

        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(es);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(es, respParser.get());
        }
    }

    @Test
    void parseSimpleString() throws Exception {
        RespSimpleString s = RespSimpleString.withUTF8("content");

        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(s);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(s, respParser.get());
        }
    }

    @Test
    void parseEmptyError() throws Exception {
        RespError es = RespError.withUTF8("");
        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(es);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(es, respParser.get());
        }
    }

    @Test
    void parseError() throws Exception {
        RespError es = RespError.withUTF8("error msg");

        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(es);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(es, respParser.get());
        }
    }

    @Test
    void parseInteger() throws Exception {
        RespInteger i = RespInteger.with(20);
        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(i);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(i, respParser.get());
        }
    }

    @Test
    void parseNullBulkString() throws Exception {
        RespBulkString bs = RespBulkString.nullBulkString();

        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(bs);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(bs, respParser.get());
        }
    }

    @Test
    void parseBulkString() throws Exception {
        RespBulkString bs = RespBulkString.with("测试".getBytes());
        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(bs);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(bs, respParser.get());
        }
    }

    @Test
    void parseEmptyArray() throws Exception {
        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            respWriter.append(RespArray.empty());
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(RespArray.empty(), respParser.get());
        }
    }

    @Test
    void parseArray() throws Exception {
        try (SeekableByteChannel writeByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
            RespWriter respWriter = RespWriter.with(writeByteChannel);
            RespSimpleString s = RespSimpleString.withUTF8("simple string");
            RespInteger i = RespInteger.with(2);
            RespBulkString bs = RespBulkString.with("bulk string".getBytes());
            a = RespArray.with(s, i, bs);
            respWriter.append(a);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            RespParser respParser = RespParser.create(channel);
            Assertions.assertEquals(a, respParser.get());
        }
    }
}