package io.github.parliament.files;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;

public interface FileService {
    boolean exists(Path file);

    void createDir(Path path) throws Exception;

    void createFileIfNotExists(Path file) throws Exception;

    byte[] readAll(Path file) throws Exception;

    void writeAll(Path file, ByteBuffer content) throws Exception;

    void overwriteAll(Path seqFilePath, ByteBuffer array) throws Exception;

    WritableByteChannel newWritableByteChannel(Path file) throws IOException;

    SeekableByteChannel newReadOnlySeekableByteChannel(Path file) throws IOException;

    FileOutputStream newOutputstream(Path file) throws IOException;

    FileInputStream newInputStream(Path file) throws IOException;

    void delete(Path roundFile) throws IOException;
}
