package io.github.parliament.page;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
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

public class Pager {
    public static final int MAX_HEAP_SIZE = 1024 * 1024 * 1024;
    public static final String PAGE_SEQ_FILENAME = "page_seq";
    public static final String METAINF_FILENAME = "metainf";
    public static final String HEAP_FILENAME_PREFIX = "heap";
    public static final int PAGE_HEAD_SIZE = 8;

    @Getter
    private Path path;
    @Getter
    private int heapSize;
    @Getter
    private int pageSize;
    @Getter
    private int pagesInHeap;
    private ConcurrentMap<Integer, Heap> heaps = new MapMaker().weakValues().makeMap();

    public static void init(Path path, int heapSize, int pageSize) throws IOException {
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        Preconditions.checkArgument(heapSize > 0);
        Preconditions.checkArgument(pageSize > 0);

        int heads = Pager.maxPagesInHeap(heapSize, pageSize);
        Preconditions.checkState(heads > 0, "heap size is too small.");

        if (Files.exists(path.resolve(METAINF_FILENAME))) {
            return;
        }
        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(METAINF_FILENAME),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {

            ByteBuffer src = ByteBuffer.allocate(4).putInt(heapSize).flip();
            while (src.hasRemaining()) {
                chn.write(src);
            }
            src = ByteBuffer.allocate(4).putInt(pageSize).flip();
            while (src.hasRemaining()) {
                chn.write(src);
            }
        }

        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(PAGE_SEQ_FILENAME),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer src = ByteBuffer.allocate(4).putInt(0).flip();
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
        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(METAINF_FILENAME), StandardOpenOption.READ)) {
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
    }

    public Page page(Integer pn) throws IOException {
        Heap heap = heap(pn);
        if (heap == null) {
            return null;
        }
        return heap.page(pn);
    }

    public Page allocate() throws IOException {
        int pn = getAndIncrement();
        Heap heap = allocateHeap(pn);
        return heap.allocate(pn);
    }

    public void sync(Page page) throws IOException {
        Heap heap = allocateHeap(page.getNo());
        heap.sync(page);
    }

    private int getAndIncrement() throws IOException {
        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(PAGE_SEQ_FILENAME),
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
            ByteBuffer dst = ByteBuffer.allocate(4);
            int read = 0;
            while (read != -1 && dst.hasRemaining()) {
                chn.read(dst);
            }
            int i = dst.flip().getInt();
            dst.clear().putInt(i + 1).flip();
            chn.position(0);
            while (dst.hasRemaining()) {
                chn.write(dst);
            }
            return i;
        }
    }

    Heap heap(int pageNo) throws IOException {
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

    Heap allocateHeap(int pageNo) throws IOException {
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

    int getHeapNoOfPage(int pageNo) {
        return (int) Math.ceil(1.0 * pageNo / pagesInHeap);
    }

    Path getHeapPath(int heap) {
        return path.resolve(HEAP_FILENAME_PREFIX + heap);
    }

    void initHeap(Path heap, int startPage) throws IOException {
        try (SeekableByteChannel chn = Files.newByteChannel(heap,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            int pn = 0;
            ByteBuffer src = ByteBuffer.allocate(4);
            while (pn < pagesInHeap) {
                src.clear().putInt(startPage + pn).flip();
                while (src.hasRemaining()) {
                    chn.write(src);
                }
                src.clear().putInt(-1).flip();
                while (src.hasRemaining()) {
                    chn.write(src);
                }
                pn++;
            }
        }
    }

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
            allocateSpaceFor(head);

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

        private Head allocateSpaceFor(Head head) throws IOException {
            int location = (int) Files.size(heapPath);
            try (SeekableByteChannel chn = Files.newByteChannel(heapPath, StandardOpenOption.APPEND)) {
                ByteBuffer buf = ByteBuffer.allocate(pageSize);
                byte b = (byte) 0xff;
                while (buf.hasRemaining()) {
                    buf.put(b);
                }
                buf.flip();
                while (buf.hasRemaining()) {
                    chn.write(buf);
                }
            }

            head.location(location);
            syncHeads();
            return head;
        }

        private void sync(Page page) throws IOException {
            try (SeekableByteChannel chn = Files.newByteChannel(heapPath, StandardOpenOption.WRITE)) {
                int loc = getHead(page.getNo()).getLocation();
                Preconditions.checkArgument(loc > 0);
                chn.position(loc);
                ByteBuffer src = ByteBuffer.wrap(page.getContent());
                while (src.hasRemaining()) {
                    chn.write(src);
                }
            }
        }

        private void syncHeads() throws IOException {
            try (SeekableByteChannel chn = Files.newByteChannel(heapPath, StandardOpenOption.WRITE)) {
                chn.position(0);
                ByteBuffer buf = ByteBuffer.allocate(headsOffset);
                heads.forEach((k, v) -> {
                    Preconditions.checkState(k == v.no);
                    buf.putInt(v.no);
                    buf.putInt(v.location);
                });

                Preconditions.checkState(buf.limit() == headsOffset);
                buf.flip();
                while (buf.hasRemaining()) {
                    chn.write(buf);
                }
            }
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

        public Head(int pn, int loc) {
            this.no = pn;
            this.location = loc;
        }

        void location(int loc) {
            this.location = loc;
        }
    }
}
