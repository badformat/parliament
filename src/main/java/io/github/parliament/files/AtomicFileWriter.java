package io.github.parliament.files;

import com.google.common.base.Preconditions;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * @author zy
 **/
public class AtomicFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(AtomicFileWriter.class);
    private Path dir;

    @Builder
    AtomicFileWriter(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
    }

    public void write(Path fileToWrite, int position, byte[] content) throws IOException {
        Preconditions.checkArgument(!fileToWrite.toAbsolutePath().startsWith(dir.toAbsolutePath()), "Can't write file in log directory");

        Path log = log(fileToWrite, position, content);

        try (SeekableByteChannel chn = Files.newByteChannel(fileToWrite, StandardOpenOption.WRITE)) {
            chn.position(position);
            ByteBuffer src = ByteBuffer.wrap(content);
            while (src.hasRemaining()) {
                chn.write(src);
            }
        }

        Files.delete(log);
    }

    public void recovery() throws IOException {
        Stream<Path> files = Files.walk(dir);
        files.forEach((f) -> {
            if (Files.isDirectory(f)) {
                return;
            }
            try {
                recovery0(f);
            } catch (IOException e) {
                logger.error("recovery failed.", e);
            }
        });
    }

    Path log(Path fileToWrite, int position, byte[] content) throws IOException {
        String logFile = String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong()));
        Path path = dir.resolve(logFile);
        SeekableByteChannel log = Files.newByteChannel(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        ByteBuffer finish = ByteBuffer.allocate(1);
        // 初始化为0x00，0xff表示写入完成
        byte b = 0x00;
        finish.put(b).clear();
        while (finish.hasRemaining()) {
            log.write(finish);
        }

        String fileName = fileToWrite.toAbsolutePath().toString();
        ByteBuffer fileNameLenBuf = ByteBuffer.allocate(4).putInt(fileName.getBytes().length).clear();
        while (fileNameLenBuf.hasRemaining()) {
            log.write(fileNameLenBuf);
        }

        ByteBuffer fileNameBuf = ByteBuffer.wrap(fileName.getBytes());
        while (fileNameBuf.hasRemaining()) {
            log.write(fileNameBuf);
        }

        ByteBuffer positionBuf = ByteBuffer.allocate(4).putInt(position).clear();
        while (positionBuf.hasRemaining()) {
            log.write(positionBuf);
        }

        ByteBuffer contentLenBuf = ByteBuffer.allocate(4).putInt(content.length).clear();
        while (contentLenBuf.hasRemaining()) {
            log.write(contentLenBuf);
        }

        ByteBuffer contentBuf = ByteBuffer.wrap(content);
        while (contentBuf.hasRemaining()) {
            log.write(contentBuf);
        }

        log.position(0);
        finish.clear().put((byte) 0xff).clear();
        while (finish.hasRemaining()) {
            log.write(finish);
        }

        return path;
    }

    private void recovery0(Path log) throws IOException {
        try (SeekableByteChannel chn = Files.newByteChannel(log, StandardOpenOption.READ)) {
            ByteBuffer finish = ByteBuffer.allocate(1);
            while (finish.hasRemaining()) {
                chn.read(finish);
            }
            if (!Objects.equals((byte) 0xff, finish.clear().get())) {
                Files.delete(log);
                return;
            }

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

            ByteBuffer pos = ByteBuffer.allocate(4);
            while (pos.hasRemaining()) {
                chn.read(pos);
            }
            pos.flip();

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

            byte[] content = new byte[contentLen.flip().getInt()];
            contentBuf.get(content);

            byte[] dst = new byte[fileName.capacity()];
            fileName.get(dst);
            try (SeekableByteChannel f = Files.newByteChannel(Paths.get(new String(dst)), StandardOpenOption.WRITE)) {
                f.position(pos.getInt());
                ByteBuffer src = ByteBuffer.wrap(content);
                while (src.hasRemaining()) {
                    f.write(src);
                }
            }

            Files.delete(log);
        }
    }
}
