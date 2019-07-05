package io.github.parliament.skiplist;

import io.github.parliament.page.Page;
import io.github.parliament.page.Pager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {
    private static String dir = "./testdir";
    private static int level = 4;

    private SkipList skipList;
    private Pager pager = new Pager(dir);

    @BeforeEach
    void beforeEach() throws IOException {
        skipList = new SkipList();
        skipList.createMetaInf(dir, level);
        skipList.setPager(pager);

        byte[] bytes = Files.readAllBytes(Paths.get(dir, "skiplist.mf"));
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        assertEquals(level, buf.getInt());
        assertEquals(-1, buf.getInt());
    }

    @Test
    void firstPut() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();

        skipList.put(key, value);

        byte[] bytes = Files.readAllBytes(Paths.get(dir, "skiplist.mf"));
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        assertEquals(level, buf.getInt());

        int pageNo = buf.getInt();
        Page page = pager.page(pageNo);
        // level
        assertEquals(1, page.getInt());
        // size
        assertEquals(1, page.getInt());
        // right page no
        assertEquals(-1, page.getInt());
        // length of last key
        assertEquals(key.length, page.getInt());
        // last key
        assertArrayEquals(key, page.getBytes(key.length));
        // first key length and first key
        assertEquals(key.length, page.getInt());
        assertArrayEquals(key, page.getBytes(key.length));
        // first value length and first value
        assertEquals(value.length, page.getInt());
        assertArrayEquals(value, page.getBytes(value.length));
    }
}
