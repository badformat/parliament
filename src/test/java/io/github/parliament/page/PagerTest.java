package io.github.parliament.page;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.*;

class PagerTest {
    private static String dir = "./testdir";
    private static Path path = Paths.get(dir);

    @BeforeEach
    void beforeEach() throws IOException {
        Pager.init(path, 8 * 1024, 1024);
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @Test
    void constructor() throws IOException {
        Pager pager = Pager.builder().path(path).build();
        assertEquals(8 * 1024, pager.getHeapSize());
        assertEquals(1024, pager.getPageSize());
    }

    @Test
    void getHeapOfPage() throws IOException {
        Pager pager = Pager.builder().path(path).build();
        Pager.Heap heap = pager.getOrCreateHeap(0);
        byte[] bytes = Files.readAllBytes(pager.getHeapPath(0));
        assertEquals(8 * 1024 / (8 + 1024), bytes.length / 8);

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int i = 0;
        while (buf.hasRemaining()) {
            assertEquals(i, buf.getInt());
            assertEquals(-1, buf.getInt());
            i++;
        }

        assertEquals(path.resolve("heap0"), heap.path());
    }

    @Test
    void allocate() throws IOException {
        Pager pager = Pager.builder().path(path).build();
        Page page = pager.allocate();

        Pager.Heap heap = pager.getOrCreateHeap(page.getNo());

        Pager.Head head = heap.getHead(page.getNo());

        assertEquals(0, head.getNo());
        assertEquals(heap.headsOffset(), head.getLocation());
    }

    @Test
    void repeatAllocate() throws IOException {
        Pager pager = Pager.builder().path(path).build();
        Stream.iterate(0, i -> i + 1).limit(20).forEach((i) -> {
            Page page = null;
            try {
                page = pager.allocate();
                assertEquals(i.intValue(), page.getNo());
            } catch (IOException e) {
                fail(e);
            }
        });
    }

    @Test
    void recycle() throws IOException {
        Pager pager = Pager.builder().path(path).build();
        Page page = pager.allocate();
        pager.recycle(page);
        assertEquals(12, Files.size(pager.getPath().resolve(Pager.METAINF_FILENAME)));

        pager.allocate();
        assertEquals(8, Files.size(pager.getPath().resolve(Pager.METAINF_FILENAME)));
    }

    @Test
    void repeatAllocateAndRecycle() throws IOException {
        Pager pager = Pager.builder().path(path).build();

        List<Page> pages = Stream.iterate(0, i -> i + 1).limit(2000).map(i -> {
            try {
                return pager.allocate();
            } catch (IOException e) {
                fail(e);
                return null;
            }
        }).collect(Collectors.toList());

        pages.forEach(page -> {
            try {
                pager.recycle(page);
            } catch (IOException e) {
                fail(e);
            }
        });

        assertEquals(8008, Files.size(pager.getPath().resolve(Pager.METAINF_FILENAME)));

        pages.forEach(page -> {
            try {
                pager.allocate();
            } catch (IOException e) {
                fail(e);
            }
        });

        assertEquals(8, Files.size(pager.getPath().resolve(Pager.METAINF_FILENAME)));
    }
}
