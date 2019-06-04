package io.github.parliament;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PagePersistenceTest {
    private static PagePersistence persistence;

    @BeforeAll
    static void beforeAll() throws IOException {
        persistence = PagePersistence.builder().path(Paths.get("./pages")).build();
    }

    @AfterAll
    static void afterAll() throws IOException {
        Files.walk(Paths.get("./pages"))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void put() throws IOException {
        persistence.put("key".getBytes(), "value".getBytes());
        assertArrayEquals(persistence.get("key".getBytes()), "value".getBytes());
        persistence.put("key".getBytes(), "value1".getBytes());
        assertArrayEquals(persistence.get("key".getBytes()), "value1".getBytes());
    }

    @Test
    void get() throws IOException {
        assertNull(persistence.get("key".getBytes()));
        persistence.put("key".getBytes(), "value".getBytes());
        assertArrayEquals(persistence.get("key".getBytes()), "value".getBytes());
    }

    @Test
    void remove() throws IOException {
        persistence.put("key".getBytes(), "value".getBytes());
        assertArrayEquals(persistence.get("key".getBytes()), "value".getBytes());
        persistence.remove("key".getBytes());
        assertNull(persistence.get("key".getBytes()));
    }
}