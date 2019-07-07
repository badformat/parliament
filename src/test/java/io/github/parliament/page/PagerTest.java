package io.github.parliament.page;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PagerTest {
    private static String dir = "./testdir";
    private static Path path = Paths.get(dir);

    @BeforeAll
    static void beforeEach() throws IOException {
        Pager.init(path, 8 * 1024, 1024);
    }

    @AfterAll
    static void afterEach() throws IOException {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @Test
    void constructor() throws IOException {
        Pager pager = new Pager(dir);
        assertEquals(8 * 1024, pager.getHeapSize());
        assertEquals(1024, pager.getPageSize());
    }
}
