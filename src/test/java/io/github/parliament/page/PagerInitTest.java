package io.github.parliament.page;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

class PagerInitTest {
    private Pager pager;
    private String dir = "./testpager";
    private Path path = Paths.get(dir);

    @BeforeEach
    void beforeEach() throws IOException {
        Files.createDirectory(path);
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @Test
    void init() throws IOException {
        Pager.init(path, 4096, 1024);
        Path heap = path.resolve("heap0");
        byte[] bytes = Files.readAllBytes(heap);
        assertEquals(bytes.length / 8, 4096 / (8 + 1024));

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        while (buf.hasRemaining()) {
            assertEquals(-1, buf.getInt());
        }

        Path seq = path.resolve(Pager.PAGE_SEQ_FILENAME);

        buf = ByteBuffer.wrap(Files.readAllBytes(seq));
        assertEquals(0, buf.getInt());
        
        Path metainf = path.resolve(Pager.METAINF_FILENAME);

        buf = ByteBuffer.wrap(Files.readAllBytes(metainf));
        assertEquals(4096, buf.getInt());
        assertEquals(1024, buf.getInt());
    }

    @Test
    void maxPagesInHeap() {
        assertEquals(1, Pager.maxPagesInHeap(16, 7));
        assertEquals(1, 10 / 6);
    }
}