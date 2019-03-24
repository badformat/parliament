package io.github.parliament.files;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DefaultFileService implements FileService {

    @Override
    public void createDir(Path path) throws Exception {
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }

    @Override
    public void createFile(Path file) throws Exception {
        if (!Files.exists(file)) {
            Files.createFile(file);
        }
    }

    @Override
    public byte[] readAll(Path file) throws Exception {
        return Files.readAllBytes(file);
    }

    @Override
    public void writeAll(Path file, ByteBuffer content) throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC, StandardOpenOption.CREATE_NEW)) {
            channel.write(content);
        }
    }

    @Override
    public void overwriteAll(Path file, ByteBuffer content) throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC)) {
            channel.write(content);
        }
    }

    @Override
    public boolean exists(Path file) {
        return Files.exists(file);
    }

    @Override
    public WritableByteChannel newWritableByteChannel(Path file) throws IOException {
        return Files.newByteChannel(file, StandardOpenOption.WRITE);
    }

    @Override
    public SeekableByteChannel newReadOnlySeekableByteChannel(Path file) throws IOException {
        return Files.newByteChannel(file, StandardOpenOption.READ);
    }

}
