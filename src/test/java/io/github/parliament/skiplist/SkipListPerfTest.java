package io.github.parliament.skiplist;

import io.github.parliament.page.Pager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author zy
 **/
class SkipListPerfTest {
    private static String dir = "./testdir";
    private static Path path = Paths.get(dir);
    private static int level = 6;

    private SkipList skipList;
    private Pager pager;
    private ThreadLocalRandom random = ThreadLocalRandom.current();

    @BeforeEach
    void beforeEach() throws IOException {
        Pager.init(path, Pager.MAX_HEAP_SIZE, 4 * 1024);
        pager = Pager.builder().path(path).build();

        SkipList.init(path, level, pager);
        skipList = SkipList.builder().path(path).pager(pager).build();
        skipList.setCheckAfterPut(true);
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @Test
    void perfPutDel() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Callable<Boolean> f = () -> {
            try {
                perfPutDel0();
            } catch (IOException | ExecutionException e) {
                fail(e);
                return false;
            }
            return true;
        };
        Future<Boolean> f1 = executor.submit(f);
        Future<Boolean> f2 = executor.submit(f);
        assertTrue(f1.get());
        assertTrue(f2.get());
    }

    void perfPutDel0() throws IOException, ExecutionException, InterruptedException {
        Set<String> keys = new HashSet<>();
        for (int i = 0; i <= 200; i++) {
            String key = String.valueOf(random.nextInt());
            String value = String.valueOf(random.nextInt());
            skipList.put(key.getBytes(), value.getBytes());
            keys.add(key);
        }
        for (String key : keys) {
            skipList.del(key.getBytes());
        }
    }
}