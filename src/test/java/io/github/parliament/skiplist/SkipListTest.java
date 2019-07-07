package io.github.parliament.skiplist;

import io.github.parliament.page.Page;
import io.github.parliament.page.Pager;
import lombok.Getter;

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
    private Pager pager;

    @BeforeEach
    void beforeEach() throws IOException {
        pager = new Pager(dir);
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
        assertEquals(1, page.getNo());
        // size
        assertEquals(1, page.getNo());
        // right page no
        assertEquals(-1, page.getNo());
        // length of last key
        assertEquals(key.length, page.getNo());
        // last key
        assertArrayEquals(key, page.getBytes(key.length));
        // first key length and first key
        assertEquals(key.length, page.getNo());
        assertArrayEquals(key, page.getBytes(key.length));
        // first value length and first value
        assertEquals(value.length, page.getNo());
        assertArrayEquals(value, page.getBytes(value.length));
    }
}
