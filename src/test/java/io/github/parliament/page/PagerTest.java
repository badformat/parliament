package io.github.parliament.page;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PagerTest {
    private Pager pager;

    @BeforeEach
    void beforeEach() {
        pager = new Pager("./testdir");
    }

    @Test
    void allocate() {
        Page page = pager.allocate();
    }
}