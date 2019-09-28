package io.github.parliament.skiplist;

import io.github.parliament.page.Page;
import io.github.parliament.page.Pager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {
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
        skipList.setCheckAfterPut(true);
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @Test
    void init() throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(dir, "skiplist.mf"));
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        assertEquals(level, buf.getInt());
        int lv = 0;
        while (lv < level) {
            assertEquals(lv, buf.getInt());
            lv++;
        }
    }

    @Test
    void put() throws IOException, ExecutionException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        skipList.put(key, value);

        SkipList.SkipListPage sp = skipList.getSkipListPages().get(0);

        Page page = pager.page(skipList.getStartPages()[0]);

        ByteBuffer buf = ByteBuffer.wrap(page.getContent());

        byte firstLevel = 0x00;
        byte meta = buf.get();
        assertEquals(firstLevel, meta & 0x0f);

        int rightPageNo = -1;
        assertEquals(rightPageNo, buf.getInt());

        int keys = 1;
        assertEquals(keys, buf.getInt());

        int keyLen = key.length;
        assertEquals(keyLen, buf.getInt());
        byte[] dstKey = new byte[keyLen];
        buf.get(dstKey);
        assertArrayEquals(key, dstKey);

        int valLen = value.length;
        assertEquals(valLen, buf.getInt());
        byte[] dstValue = new byte[valLen];
        buf.get(dstValue);
        assertArrayEquals(value, dstValue);
    }

    @Test
    void putTooLongValue() {
        byte[] key = "key".getBytes();
        byte[] value = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit" +
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit" +
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit" +
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit" +
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit").getBytes();

        assertThrows(KeyValueTooLongException.class, () -> skipList.put(key, value));
    }

    @Test
    void splitAfterPut() throws IOException, ExecutionException {
        skipList.setAlwaysPromo(true);
        int limit = 40;
        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(i).getBytes();
            byte[] value = ("Lorem ipsum " + i).getBytes();
            skipList.put(key, value);
            assertEquals(value, skipList.get(key));
        }

        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(i).getBytes();
            byte[] value = ("Lorem ipsum " + i).getBytes();
            assertArrayEquals(value, skipList.get(key));
        }
    }

    @Test
    void promoAfterPut() throws IOException, ExecutionException {
        int limit = 40;
        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(random.nextInt()).getBytes();
            byte[] value = ("Lorem ipsum " + random.nextInt()).getBytes();
            skipList.put(key, value);
            assertEquals(value, skipList.get(key));
        }
    }

    @Test
    void get() throws IOException, ExecutionException {
        skipList.put("key".getBytes(), "value".getBytes());
        assertArrayEquals("value".getBytes(), skipList.get("key".getBytes()));
    }

    @Test
    void repeatGet() throws IOException, ExecutionException {
        skipList.setCheckAfterPut(false);
        List<byte[]> keys = new ArrayList<>();
        Map<byte[], byte[]> kvs = new HashMap<>();

        int limit = 40;
        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(random.nextInt()).getBytes();
            byte[] value = ("Lorem ipsum " + random.nextInt()).getBytes();
            skipList.put(key, value);
            keys.add(key);
            kvs.put(key, value);
        }

        keys.forEach(k -> {
            try {
                byte[] value = skipList.get(k);
                assertArrayEquals(kvs.get(k), value);
            } catch (IOException | ExecutionException e) {
                fail(e);
            }
        });
    }

    @Test
    void orderRange() throws IOException, ExecutionException {
        int limit = 9;
        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(i).getBytes();
            byte[] value = String.valueOf(i).getBytes();
            skipList.put(key, value);
        }

        List<byte[]> values = skipList.range(String.valueOf(0).getBytes(), String.valueOf(limit).getBytes());
        for (int i = 0; i < limit; i++) {
            byte[] v = values.get(i);
            assertArrayEquals(v, String.valueOf(i).getBytes());
        }
    }

    @Test
    void range() throws IOException, ExecutionException {
        int limit = 40;
        byte[] min = new byte[0];
        byte[] max = new byte[0];
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(random.nextInt()).getBytes();
            if (Arrays.compare(max, key) <= 0) {
                max = key;
            }
            byte[] value = ("Lorem ipsum " + random.nextInt()).getBytes();
            values.add(value);
            skipList.put(key, value);
            assertEquals(value, skipList.get(key));
        }

        List<byte[]> range = skipList.range(min, max);
        assertEquals(values.size() - 1, range.size());
    }

    @Test
    void rangeIllegalMinMax() throws IOException, ExecutionException {
        byte[] min = "9".getBytes();
        byte[] max = "0".getBytes();
        skipList.put(min, min);
        assertTrue(skipList.range(min, max).isEmpty());
    }

    @Test
    void rangeEmpty() throws IOException, ExecutionException {
        skipList.range("0".getBytes(), "9".getBytes());
    }

    @Test
    void del() throws IOException, ExecutionException {
        int limit = 9;
        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(i).getBytes();
            byte[] value = String.valueOf(i).getBytes();
            skipList.put(key, value);
        }

        byte[] key = String.valueOf(0).getBytes();
        skipList.del(key);
        assertNull(skipList.get(key));
    }

    @Test
    void delAll() throws IOException, ExecutionException {
        int limit = 200;
        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(i).getBytes();
            byte[] value = String.valueOf(i).getBytes();
            skipList.put(key, value);
        }

        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(i).getBytes();
            skipList.del(key);
        }

        for (int i = 0; i < limit; i++) {
            byte[] key = String.valueOf(i).getBytes();

            assertNull(skipList.get(key));
        }
    }
}
