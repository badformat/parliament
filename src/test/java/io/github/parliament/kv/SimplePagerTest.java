package io.github.parliament.kv;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import io.github.parliament.DuplicateKeyException;
import io.github.parliament.page.SimplePager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SimplePagerTest {
    private static SimplePager pager;

    @BeforeEach
    void beforeEach() throws IOException {
        pager = create();
    }

    private SimplePager create() throws IOException {
        return SimplePager.builder().path("./test").build();
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.walk(Paths.get("./test"))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void insert() throws IOException, DuplicateKeyException {
        byte[] key = key(0);
        byte[] value = value(0);
        pager.insert(key, value);
        assertArrayEquals(value, pager.get(key));
    }

    @Test
    void insertSameKey() throws IOException, DuplicateKeyException {
        byte[] key = key(0);
        byte[] value = value(0);
        pager.insert(key, value);
        assertThrows(DuplicateKeyException.class, () -> {
            byte[] key1 = key(0);
            byte[] value1 = value(2);
            pager.insert(key1, value1);
        });
    }

    @Test
    void insert1() throws IOException, DuplicateKeyException {
        for (int i = 0; i < 20; i++) {
            pager.insert(key(i), value(i));
        }
        for (int i = 0; i < 20; i++) {
            assertArrayEquals(value(i), pager.get(key(i)), "error key " + key(i));
        }
    }

    @Test
    void remove() throws IOException, DuplicateKeyException {
        for (int i = 0; i < 20; i++) {
            pager.insert(key(i), value(i));
        }

        pager.remove(key(0));
        assertNull(pager.get(key(0)));
    }

    @Test
    void load() throws IOException, DuplicateKeyException {
        pager.insert(key(0), value(0));

        SimplePager pager2 = create();

        assertArrayEquals(value(0), pager2.get(key(0)));
    }

    @Test
    void range() {
    }

    private byte[] value(int i) {
        return ("default key" + i).getBytes();
    }

    private byte[] key(int i) {
        return ("default value" + i).getBytes();
    }
}