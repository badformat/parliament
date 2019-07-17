package io.github.parliament.skiplist;

import io.github.parliament.page.Pager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zy
 **/
class SkipListParallelTest {
    private static String dir = "./testdir";
    private static Path path = Paths.get(dir);
    private static int level = 6;

    private SkipList skipList;
    private Pager pager;
    private ThreadLocalRandom random = ThreadLocalRandom.current();

    @BeforeEach
    void beforeEach() throws IOException {
        Pager.init(path, 512, 64);
        pager = Pager.builder().path(path).build();

        SkipList.init(path, level, pager);
        skipList = SkipList.builder().path(path).pager(pager).build();
        skipList.setGetAfterPut(true);
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @RepeatedTest(5)
    void put() {
        Stream.iterate(1, (i) -> i + 1).limit(100).parallel()
                .map(i -> {
                    try {
                        skipList.put(String.valueOf(i).getBytes(), String.valueOf(i).getBytes());
                    } catch (IOException | ExecutionException e) {
                        fail(e);
                    }
                    return i;
                })
                .map(i -> {
                    try {
                        assertArrayEquals(String.valueOf(i).getBytes(), skipList.get(String.valueOf(i).getBytes()));
                    } catch (IOException | ExecutionException e) {
                        fail(e);
                    }
                    return i;
                }).toArray();
    }
}