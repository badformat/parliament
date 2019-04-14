package io.github.parliament.files;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
    public void createFileIfNotExists(Path file) throws Exception {
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
            while (content.hasRemaining()) {
                channel.write(content);
            }
        }
    }

    @Override
    public void overwriteAll(Path file, ByteBuffer content) throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC)) {
            while (content.hasRemaining()) {
                channel.write(content);
            }
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

    @Override
    public FileOutputStream newOutputstream(Path roundFile) throws IOException {
        return new FileOutputStream(roundFile.toFile());
    }

    @Override
    public FileInputStream newInputStream(Path file) throws IOException {
        return new FileInputStream(file.toFile());
    }

    @Override
    public void delete(Path roundFile) throws IOException {
        try {
            Files.delete(roundFile);
        } catch (NoSuchFileException e) {
            // TODO log
        }
    }

}
