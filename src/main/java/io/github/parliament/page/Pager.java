package io.github.parliament.page;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import io.github.parliament.files.AtomicFileWriter;
import lombok.Builder;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * {@link Page}管理器，负责在{@link Heap}文件中分配、获取、回收一块文件page页面。
 * {@link Page}是在某个{@link Heap}文件中的一块地址，每个Page都有独立的编号。
 * <p>
 * 一个heap文件由heads和pages两部分组成，head保存了该堆文件所有page的编号和对应的地址信息。
 * head的个数由page大小决定。
 * <p>
 * heap文件格式如下：
 * <pre>
 * |------------------head---------------------|---------pages-------|
 * | page no (4字节) | page location (4字节)|.. | page | page |...|...|
 * </pre>
 */
public class Pager {
    public static final int MAX_HEAP_SIZE = 1024 * 1024 * 1024;
    static final String PAGE_SEQ_FILENAME = "page_seq";
    static final String FREE_PAGES = "free_pages";
    private static final String HEAP_FILENAME_PREFIX = "heap";
    private static final String LOG_DIR = "log";
    private static final int PAGE_HEAD_SIZE = 8;

    @Getter
    private Path path;
    @Getter
    private int heapSize;
    @Getter
    private int pageSize;
    @Getter
    private int pagesInHeap;
    private ConcurrentMap<Integer, Heap> heaps = new MapMaker().weakValues().makeMap();
    private AtomicFileWriter atomicFileWriter;

    class Heap {
        private final ConcurrentSkipListMap<Integer, Head> heads = new ConcurrentSkipListMap<>();
        private int headsOffset;
        private Path heapPath;

        Heap(Path heapPath) throws IOException {
            this.heapPath = heapPath;
            try (SeekableByteChannel chn = Files.newByteChannel(heapPath, StandardOpenOption.READ)) {
                headsOffset = pagesInHeap * PAGE_HEAD_SIZE;
                ByteBuffer dst = ByteBuffer.allocate(headsOffset);
                int read = 0;
                while (read != -1 && dst.hasRemaining()) {
                    read = chn.read(dst);
                }
                Preconditions.checkState(dst.limit() == headsOffset);
                dst.flip();

                while (dst.hasRemaining()) {
                    int pn = dst.getInt();
                    int loc = dst.getInt();
                    heads.put(pn, new Head(pn, loc));
                }
            }
        }

        Head getHead(int no) {
            return heads.get(no);
        }

        int headsOffset() {
            return headsOffset;
        }

        Path path() {
            return heapPath;
        }

        Page page(int pn) throws IOException {
            Head head = heads.get(pn);
            int loc = head.getLocation();
            if (loc == -1) {
                return null;
            }
            byte[] bytes = pageContent(loc);
            return Page.builder().no(pn).location(head.location).content(bytes).build();
        }

        Page allocate(int pn) throws IOException {
            Head head = heads.get(pn);
            Preconditions.checkState(head != null);
            int location = head.getLocation();
            Preconditions.checkState(location == -1);
            allocateSpaceForPage(head);

            byte[] bytes = pageContent(head.location);
            return Page.builder().no(pn).location(head.location).content(bytes).build();
        }

        private byte[] pageContent(int location) throws IOException {
            try (SeekableByteChannel chn = Files.newByteChannel(heapPath, StandardOpenOption.READ)) {
                ByteBuffer dst = ByteBuffer.allocate(pageSize);
                int read = 0;
                chn.position(location);
                while (read != -1 && dst.hasRemaining()) {
                    read = chn.read(dst);
                }
                Preconditions.checkState(!dst.hasRemaining());

                byte[] bytes = new byte[pageSize];
                dst.flip();
                dst.get(bytes);
                return bytes;
            }
        }

        private void allocateSpaceForPage(Head head) throws IOException {
            int location = (int) Files.size(heapPath);

            ByteBuffer buf = ByteBuffer.allocate(pageSize);
            byte b = (byte) 0xff;
            while (buf.hasRemaining()) {
                buf.put(b);
            }
            buf.flip();

            atomicFileWriter.write(heapPath, location, buf);

            head.location(location);
            syncHeads();
        }

        private void sync(Page page) throws IOException {
            int loc = getHead(page.getNo()).getLocation();
            Preconditions.checkArgument(loc > 0);
            atomicFileWriter.write(heapPath, loc, page.getContent());
        }

        private void syncHeads() throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(headsOffset);
            heads.forEach((k, v) -> {
                Preconditions.checkState(k == v.no);
                buf.putInt(v.no);
                buf.putInt(v.location);
            });

            Preconditions.checkState(buf.limit() == headsOffset);
            buf.flip();
            atomicFileWriter.write(heapPath, 0, buf);
        }
    }

    /**
     * @author zy
     **/
    static class Head {
        @Getter
        private int no;
        @Getter
        private int location;

        private Head(int pn, int loc) {
            this.no = pn;
            this.location = loc;
        }

        void location(int loc) {
            this.location = loc;
        }
    }

    public static void init(Path path, int heapSize, int pageSize) throws IOException {
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        Preconditions.checkArgument(heapSize > 0);
        Preconditions.checkArgument(pageSize > 0);

        int heads = Pager.maxPagesInHeap(heapSize, pageSize);
        Preconditions.checkState(heads > 0, "heap size is too small.");

        if (Files.exists(path.resolve(FREE_PAGES))) {
            return;
        }
        Files.createDirectories(path.resolve(LOG_DIR));
        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(FREE_PAGES),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {

            ByteBuffer src = ByteBuffer.allocate(4);
            src.putInt(heapSize);
            src.flip();
            while (src.hasRemaining()) {
                chn.write(src);
            }
            src = ByteBuffer.allocate(4).putInt(pageSize);
            src.flip();
            while (src.hasRemaining()) {
                chn.write(src);
            }
        }

        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(PAGE_SEQ_FILENAME),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer src = ByteBuffer.allocate(4);
            src.putInt(0);
            src.flip();
            while (src.hasRemaining()) {
                chn.write(src);
            }
        }

    }

    static int maxPagesInHeap(int heapSize, int pageSize) {
        return heapSize / (PAGE_HEAD_SIZE + pageSize);
    }

    @Builder
    private Pager(Path path) throws IOException {
        this.path = path;
        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(FREE_PAGES), StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocate(8);
            int read = 0;
            while (read != -1 && dst.hasRemaining()) {
                read = chn.read(dst);
            }
            dst.flip();
            heapSize = dst.getInt();
            pageSize = dst.getInt();
            pagesInHeap = Pager.maxPagesInHeap(heapSize, pageSize);
        }
        atomicFileWriter = AtomicFileWriter.builder().dir(path.resolve(LOG_DIR)).build();
        atomicFileWriter.recovery();
    }

    public Page page(Integer pn) throws IOException {
        Heap heap = heap(pn);
        if (heap == null) {
            return null;
        }
        return heap.page(pn);
    }

    public Page allocate() throws IOException {
        int pn;
        long size;
        synchronized (FREE_PAGES) {
            size = Files.size(path.resolve(FREE_PAGES));
        }

        if (size > 8) {
            synchronized (FREE_PAGES) {
                try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(FREE_PAGES),
                        StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                    chn.position(size - 4);
                    ByteBuffer dst = ByteBuffer.allocate(4);
                    int read = 0;
                    while (read != -1 && dst.hasRemaining()) {
                        read = chn.read(dst);
                    }
                    dst.flip();
                    pn = dst.getInt();
                    chn.truncate(size - 4);
                }
            }
            Heap heap = getOrCreateHeap(pn);
            return heap.page(pn);
        } else {
            pn = getAndIncrement();
            Heap heap = getOrCreateHeap(pn);
            return heap.allocate(pn);
        }
    }

    public void recycle(Page page) throws IOException {
        synchronized (FREE_PAGES) {
            long position = Files.size(path.resolve(FREE_PAGES));
            ByteBuffer buf = ByteBuffer.allocate(4).putInt(page.getNo());
            buf.flip();
            atomicFileWriter.write(path.resolve(FREE_PAGES), position, buf);
        }
    }

    public void sync(Page page) throws IOException {
        Heap heap = getOrCreateHeap(page.getNo());
        heap.sync(page);
    }

    private int getAndIncrement() throws IOException {
        synchronized (PAGE_SEQ_FILENAME) {
            int i;
            try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(PAGE_SEQ_FILENAME), StandardOpenOption.READ)) {
                ByteBuffer dst = ByteBuffer.allocate(4);
                int read = 0;
                while (read != -1 && dst.hasRemaining()) {
                    read = chn.read(dst);
                }
                dst.flip();
                i = dst.getInt();
            }
            ByteBuffer buf = ByteBuffer.allocate(4).putInt(i + 1);
            buf.clear();
            atomicFileWriter.write(path.resolve(PAGE_SEQ_FILENAME), 0, buf);
            return i;
        }
    }

    private Heap heap(int pageNo) throws IOException {
        Preconditions.checkArgument(pageNo >= 0);
        int heap = getHeapNoOfPage(pageNo);
        Path heapPath = getHeapPath(heap);
        if (!Files.exists(heapPath)) {
            return null;
        }

        if (heaps.containsKey(heap)) {
            return heaps.get(heap);
        }

        Heap h = new Heap(heapPath);
        heaps.put(heap, h);
        return h;
    }

    Heap getOrCreateHeap(int pageNo) throws IOException {
        Preconditions.checkArgument(pageNo >= 0);
        int heap = getHeapNoOfPage(pageNo);
        Path heapPath = getHeapPath(heap);
        if (!Files.exists(heapPath)) {
            initHeap(heapPath, pageNo);
        }

        if (heaps.containsKey(heap)) {
            return heaps.get(heap);
        }

        Heap h = new Heap(heapPath);
        heaps.put(heap, h);
        return h;
    }

    private int getHeapNoOfPage(int pageNo) {
        return (int) Math.ceil(1.0 * pageNo / pagesInHeap);
    }

    Path getHeapPath(int heap) {
        return path.resolve(HEAP_FILENAME_PREFIX + heap);
    }

    private void initHeap(Path heap, int startPage) throws IOException {
        Files.createFile(heap);
        int pn = 0;
        ByteBuffer src = ByteBuffer.allocate(8 * pagesInHeap);
        while (pn < pagesInHeap) {
            src.putInt(startPage + pn);
            src.putInt(-1);
            pn++;
        }
        src.flip();
        atomicFileWriter.write(heap, 0, src);
    }
}
