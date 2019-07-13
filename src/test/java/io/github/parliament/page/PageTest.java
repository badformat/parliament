package io.github.parliament.page;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zy
 **/
class PageTest {

    @Test
    void insertBytes() {
        Page page = Page.builder().no(0).location(1).content("value".getBytes()).build();

        page.insertBytes(0, "inserted value ".getBytes());
        assertArrayEquals("inserted value value".getBytes(), page.getContent());

        page.insertBytes(page.getContent().length, " last".getBytes());
        assertArrayEquals("inserted value value last".getBytes(), page.getContent());

        page = Page.builder().no(0).location(1).content("1 3".getBytes()).build();
        page.insertBytes(1, " 2".getBytes());
        assertArrayEquals("1 2 3".getBytes(), page.getContent());
    }

    @Test
    void replaceBytes() {
        Page page = Page.builder().no(0).location(1).content("value".getBytes()).build();
        page.replaceBytes(0, "re".length(), "re".getBytes());
        assertArrayEquals("relue".getBytes(), page.getContent());

        page.replaceBytes(2, 2 + "ee".length(), "ee".getBytes());
        assertArrayEquals("reeee".getBytes(), page.getContent());
    }

    @Test
    void copyBytes() {
        Page page = Page.builder().no(0).location(1).content("value".getBytes()).build();
        byte[] dst = new byte[2];
        page.copyBytes(0, 2, dst);
        assertArrayEquals("va".getBytes(), dst);
    }
}