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
        Pager.init(path, 128, 32);
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

        Page page = pager.getOrCreatePage(0);

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
        byte[] value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit".getBytes();

        assertThrows(KeyValueTooLongException.class, () -> skipList.put(key, value));
    }

    @Test
    void splitAfterPut() throws IOException {
        for (int i = 0; i < 9; i++) {
            skipList.put(String.valueOf(i).getBytes(), ("Lorem ipsum " + i).getBytes());
        }

        skipList.sync();

        for (int i = 0; i < 9; i++) {
            Page page = skipList.findLeafPageOfKey(String.valueOf(i).getBytes());
            assertNotNull(page);

            SkipList.SkipListPage skipListPage = skipList.new SkipListPage(page);

            SkipList.SkipListPage.Node node = skipListPage.getHead();
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
        for (int i = 0; i < 9; i++) {
            skipList.put(String.valueOf(i).getBytes(), ("Lorem ipsum " + i).getBytes());
        }

        skipList.sync();

        byte[] key = String.valueOf(0).getBytes();
        Page page = skipList.findLeafPageOfKey(key);
        SkipList.SkipListPage skipListPage = skipList.new SkipListPage(page);
        skipListPage.promo(key);

        skipList.sync();

        Page leaf = skipList.findLeafPageOfKey(String.valueOf(0).getBytes());
        assertNotNull(leaf.getUpLevelPage());

        page = skipList.findPageOfKeyInLevel(1, key);


        ByteBuffer buf = ByteBuffer.wrap(page.getContent());
        byte meta = buf.get();
        assertEquals(1, meta & 0x0f);

        int rightPageNo = buf.getInt();
        assertEquals(-1, rightPageNo);

        int noOfKeys = buf.getInt();
        assertEquals(1, noOfKeys);
    }
}
