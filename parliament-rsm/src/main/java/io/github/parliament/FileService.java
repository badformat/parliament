package io.github.parliament;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface FileService {
    boolean exists(Path file);

    void createDir(Path path) throws Exception;

    void createFile(Path file) throws Exception;

    byte[] readAll(Path file) throws Exception;

    void writeAll(Path file, ByteBuffer content) throws Exception;

    void overwriteAll(Path seqFilePath, ByteBuffer array) throws Exception;

}
