package io.github.parliament.page;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zy
 **/
class PageTest {


    @Test
    void replaceBytes() {
        Page page = Page.builder().no(0).location(1).content("value".getBytes()).build();
        page.replaceBytes(0, "re".length(), "re".getBytes());
        assertArrayEquals("relue".getBytes(), page.getContent());

        page.replaceBytes(2, 2 + "ee".length(), "ee".getBytes());
        assertArrayEquals("reeee".getBytes(), page.getContent());
    }

}