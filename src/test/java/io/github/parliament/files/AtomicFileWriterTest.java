package io.github.parliament.files;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zy
 **/
class AtomicFileWriterTest {
    private AtomicFileWriter atomicFileWriter;
    private Path logPath = Paths.get("./afw");
    private Path dataPath = Paths.get("./data");

    @BeforeEach
    void setUp() throws IOException {
        atomicFileWriter = AtomicFileWriter.builder().dir(logPath).build();
        Files.createDirectory(dataPath);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(logPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        Files.walk(dataPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @Test
    void log() throws IOException {
        Path fileToWrite = dataPath.resolve("test");
        Files.createFile(fileToWrite);
        int position = 0;
        byte[] content = "content".getBytes();
        Path log = atomicFileWriter.log(fileToWrite, position, content);

        assertNotNull(log);
        assertTrue(Files.exists(log));

        try (SeekableByteChannel chn = Files.newByteChannel(log, StandardOpenOption.READ)) {

            ByteBuffer finish = ByteBuffer.allocate(1);
            while (finish.hasRemaining()) {
                chn.read(finish);
            }
            // 初始化为0x00，0xff表示写入完成
            assertEquals((byte) 0xff, finish.clear().get());

            ByteBuffer fileNameLen = ByteBuffer.allocate(4);
            while (fileNameLen.hasRemaining()) {
                chn.read(fileNameLen);
            }
            fileNameLen.flip();

            ByteBuffer fileName = ByteBuffer.allocate(fileNameLen.getInt());
            while (fileName.hasRemaining()) {
                chn.read(fileName);
            }
            fileName.flip();

            byte[] dst = new byte[fileName.capacity()];
            fileName.get(dst);
            assertEquals(fileToWrite.toAbsolutePath().toString(), new String(dst));

            ByteBuffer pos = ByteBuffer.allocate(4);
            while (pos.hasRemaining()) {
                chn.read(pos);
            }
            pos.flip();
            assertEquals(pos.getInt(), position);

            ByteBuffer contentLen = ByteBuffer.allocate(4);
            while (contentLen.hasRemaining()) {
                chn.read(contentLen);
            }
            contentLen.flip();

            ByteBuffer contentBuf = ByteBuffer.allocate(contentLen.getInt());
            while (contentBuf.hasRemaining()) {
                chn.read(contentBuf);
            }
            contentBuf.flip();

            byte[] clog = new byte[content.length];
            contentBuf.get(clog);
            assertArrayEquals(content, clog);
        }
    }

    @Test
    void write() throws IOException {
        Path fileToWrite = dataPath.resolve("test1");
        Files.createFile(fileToWrite);
        int position = 0;
        byte[] content = "write content".getBytes();

        atomicFileWriter.write(fileToWrite, position, content);

        check(fileToWrite, content);
    }

    @Test
    void writeWithPosition() throws IOException {
        Path fileToWrite = dataPath.resolve("test1");
        Files.createFile(fileToWrite);
        String content = "content";
        try (SeekableByteChannel chn = Files.newByteChannel(fileToWrite, StandardOpenOption.WRITE)) {
            ByteBuffer src = ByteBuffer.wrap(content.getBytes());
            while (src.hasRemaining()) {
                chn.write(src);
            }
        }
        String append = "append content";
        atomicFileWriter.write(fileToWrite, content.length(), append.getBytes());

        check(fileToWrite, (content + append).getBytes());
    }

    @Test
    void recovery() throws IOException {
        Path fileToWrite = dataPath.resolve("test2");
        Files.createFile(fileToWrite);
        int position = 0;
        byte[] content = "write content".getBytes();
        atomicFileWriter.log(fileToWrite, position, content);

        atomicFileWriter.recovery();

        check(fileToWrite, content);
    }

    void check(Path fileToWrite, byte[] content) throws IOException {
        byte[] bytes = Files.readAllBytes(fileToWrite);
        assertArrayEquals(content, bytes);
    }
}