package io.github.parliament.rsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 * @author zy
 */
public class FileIncreamentTest {
    private Path            file            = Paths.get("./tmptest", "seq");
    private ExecutorService executorService = Executors.newFixedThreadPool(10);

    @BeforeEach
    void beforeEach() throws IOException {
        Files.createDirectories(Paths.get("./tmptest"));
        Files.createFile(file);
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.delete(file);
    }

    private int readIntFromFile(Path file) throws IOException {
        byte[] b = Files.readAllBytes(file);
        if (b == null || b.length == 0) {
            return 0;
        }

        return Integer.valueOf(new String(b));
    }

    synchronized private int getAndIncreament(Path file) throws IOException {
        int no = readIntFromFile(file);
        int i = no + 1;
        Files.write(file, String.valueOf(i).getBytes());
        return i;
    }

    @Test
    void test() {
        int limit = 23;
        Set<Integer> s = Collections.synchronizedSet(new HashSet<>());

        Stream.iterate(0, (i) -> i + 1).limit(limit).parallel().peek((i) -> {
            try {
                s.add(getAndIncreament(file));
            } catch (IOException e) {
                fail(e);
            }
        }).count();

        assertEquals(limit, s.size());
    }

    @Test
    void test2() {
        int limit = 23;
        Set<Integer> s = Collections.synchronizedSet(new HashSet<>());

        Stream.iterate(0, (i) -> i + 1).limit(limit).parallel().peek((i) -> {
            try {
                int x = getAndIncreament(file);
                executorService.submit(() -> {
                    s.add(x);
                }).get();
            } catch (InterruptedException | ExecutionException | IOException e) {
                fail(e);
            }
        }).count();

        assertEquals(limit, s.size());
    }
}