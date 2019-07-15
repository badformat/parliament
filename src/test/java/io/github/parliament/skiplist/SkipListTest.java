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
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {
    private static String dir = "./testdir";
    private static Path path = Paths.get(dir);
    private static int level = 4;

    private SkipList skipList;
    private Pager pager;

    @BeforeEach
    void beforeEach() throws IOException {
        Pager.init(path, 512, 128);
        pager = Pager.builder().path(path).build();

        SkipList.init(path, level, pager);
        skipList = SkipList.builder().path(path).pager(pager).build();
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
    void put() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();

        skipList.put(key, value);

        skipList.sync();

        Page page = pager.page(0);

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
    void split() throws IOException {
//        SkipList.SkipListPage page；
    }

    @Test
    void putTooLongValue() {
        byte[] key = "key".getBytes();
        byte[] value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit".getBytes();

        assertThrows(KeyValueTooLongException.class, () -> skipList.put(key, value));
    }

    void checkSkipListPages() throws IOException {
        int height = skipList.getHeight();
        for (int i = 0; i < height; i++) {
            int p = skipList.getStartPages()[i];
            Page dp = pager.page(p);
            SkipList.SkipListPage sp = skipList.new SkipListPage(dp);

        }
    }

    @Test
    void splitAfterPut() throws IOException {
        for (int i = 0; i < 90; i++) {
            skipList.put(String.valueOf(i).getBytes(), ("Lorem ipsum " + i).getBytes());
        }

        skipList.sync();

        for (int i = 0; i < 90; i++) {
            SkipList.SkipListPage page = skipList.findLeafSkipListPageOfKey(String.valueOf(i).getBytes());
            assertNotNull(page);

            SkipList.SkipListPage.Node node = page.getHead();
            assertNotNull(node);

            while (node != null) {
                String key = new String(node.getKey());
                assertEquals("Lorem ipsum " + key, new String(node.getValue()));
                node = node.getNext();
            }
        }
    }

    @Test
    void promoAfterPut() throws IOException {
        for (int i = 0; i < 90; i++) {
            Instant begin = Instant.now();
            skipList.put(String.valueOf(i).getBytes(), ("Lorem ipsum " + i).getBytes());
            Instant end = Instant.now();

            System.out.println(Duration.between(begin, end).toMillis());
        }

        skipList.sync();

        byte[] key = String.valueOf(0).getBytes();

        SkipList.SkipListPage leaf = skipList.findLeafSkipListPageOfKey(key);
        assertNotNull(leaf.getSuperPage());

        int i = 0;
        SkipList.SkipListPage.Node node = leaf.getHead();
        while (node != null) {
            i++;
            node = node.getNext();
        }
        assertTrue(i < 9);
        assertTrue(i >= 1);
    }
}
