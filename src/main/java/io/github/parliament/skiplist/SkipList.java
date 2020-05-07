package io.github.parliament.skiplist;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.UnsignedBytes;
import io.github.parliament.Persistence;
import io.github.parliament.page.Page;
import io.github.parliament.page.Pager;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * skip list持久化实现。
 */
public class SkipList implements Persistence {
    private static final Logger logger = LoggerFactory.getLogger(SkipList.class);
    static final String META_FILE_NAME = "skiplist.mf";
    static final int HEAD_SIZE_IN_PAGE = 1 + 4 + 4;

    private Pager pager;
    @Getter
    private Path path;
    private Path metaFilePath;
    @Getter
    private int height;
    @Getter(AccessLevel.PACKAGE)
    private Map<Integer, Integer> startPages;
    @Getter(AccessLevel.PACKAGE)
    private final LoadingCache<Integer, SkipListPage> skipListPages = CacheBuilder
            .newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .build(new CacheLoader<Integer, SkipListPage>() {
                @Override
                public SkipListPage load(Integer pn) throws Exception {
                    return new SkipListPage(pager.page(pn));
                }
            });

    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private boolean alwaysPromo = false;

    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private boolean checkAfterPut = false;

    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();

    public static void init(Path path, int height, Pager pager) throws IOException {
        Path metaFilePath = path.resolve(META_FILE_NAME);
        if (Files.exists(metaFilePath)) {
            return;
        }
        Preconditions.checkArgument(height > 0);
        Preconditions.checkArgument(height <= 0x0f);

        try (SeekableByteChannel chn = Files.newByteChannel(metaFilePath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer src = ByteBuffer.allocate(4).putInt(height);
            src.flip();
            while (src.hasRemaining()) {
                chn.write(src);
            }

            int level = 0;
            while (level < height) {
                Page page = pager.allocate();
                // init height
                page.replaceBytes(0, 1, ByteBuffer.allocate(1).put((byte) level).array());
                // init right page
                page.replaceBytes(1, 5, ByteBuffer.allocate(4).putInt(-1).array());
                // init number of keys
                page.replaceBytes(5, 9, ByteBuffer.allocate(4).putInt(0).array());
                pager.sync(page);

                src.clear();
                src.putInt(page.getNo());
                src.flip();
                while (src.hasRemaining()) {
                    chn.write(src);
                }
                level++;
            }
        }
    }

    @EqualsAndHashCode
    @ToString
    class SkipListPage {
        @Getter
        private volatile Page page;
        @Getter
        private volatile byte meta;
        @Getter
        private volatile int level;
        @Getter
        private volatile int size;
        @Getter
        @Setter
        private volatile int rightPageNo;
        @Getter
        private volatile boolean isLeaf;
        @Getter
        @ToString.Exclude
        private volatile ConcurrentNavigableMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(bytesComparator);

        SkipListPage(Page page) {
            this.page = page;
            ByteBuffer buf = ByteBuffer.wrap(page.getContent());
            meta = buf.get();
            level = meta & 0x0f;
            isLeaf = (level == 0);
            rightPageNo = buf.getInt();
            int keys = buf.getInt();
            size = HEAD_SIZE_IN_PAGE;
            if (keys > 0) {
                int cur = keys;
                while (cur > 0) {
                    int keyLen = buf.getInt();
                    size += 4;

                    byte[] key = new byte[keyLen];
                    buf.get(key);
                    size += keyLen;

                    int valueLen = 0;
                    byte[] value = null;

                    if (isLeaf) {
                        valueLen = buf.getInt();
                        value = new byte[valueLen];
                        size += 4;
                    } else {
                        valueLen = 4;
                        value = new byte[valueLen];
                    }
                    buf.get(value);
                    size += valueLen;

                    map.put(key, value);
                    cur--;
                }
            }
        }

        void put(byte[] key, byte[] value) throws IOException, ExecutionException {
            int space = 4 + key.length + value.length;
            if (isLeaf) {
                // value length field.
                space += 4;
            } else if (value.length != 4) {
                throw new IllegalStateException();
            }

            if (space > pager.getPageSize()) {
                throw new KeyValueTooLongException("Can't put the key and value in one page.");
            }
            map.put(key, value);
            update();

            promo(key);
        }

        byte[] get(byte[] key) {
            return map.get(key);
        }

        boolean del(byte[] key) throws IOException, ExecutionException {
            boolean d = map.remove(key) != null;
            if (d) {
                update();
            }
            return d;
        }

        void promo(byte[] key) throws IOException, ExecutionException {
            if (!alwaysPromo) {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                if (random.nextBoolean()) {
                    return;
                }
            }

            Preconditions.checkState(level < SkipList.this.height);
            if (level == SkipList.this.height - 1) {
                return;
            }

            SkipListPage parent = findSkipListPageForKey(key, level + 1);
            byte[] value = ByteBuffer.allocate(4).putInt(page.getNo()).array();
            parent.put(key, value);
        }

        private void update() throws IOException, ExecutionException {
            int s = HEAD_SIZE_IN_PAGE;
            int k = 0;

            for (byte[] key : map.keySet()) {
                s += key.length;
                s += 4;
                s += map.get(key).length;
                if (isLeaf) {
                    s += 4;
                }
                k++;
            }
            size = s;

            if (remaining() < 0) {
                // check left size
                // allocate a new page and insert it into list
                split();
            } else {
                sync();
            }
        }

        private int remaining() {
            return pager.getPageSize() - this.getSize();
        }

        private void split() throws IOException, ExecutionException {
            Preconditions.checkState(!map.isEmpty());
            Page p = SkipList.this.allocatePage(level);
            skipListPages.invalidate(p.getNo());
            SkipListPage newPage = skipListPages.get(p.getNo());

            newPage.map = new ConcurrentSkipListMap<>(map.tailMap(map.firstKey(), false));
            Preconditions.checkState(!map.isEmpty());
            map = new ConcurrentSkipListMap<>(map.headMap(map.firstKey(), true));
            newPage.rightPageNo = this.rightPageNo;
            this.rightPageNo = newPage.getPage().getNo();

            this.update();
            newPage.update();

            if (this.isLeaf ^ newPage.isLeaf) {
                throw new IllegalStateException();
            }

            Preconditions.checkState(pager.getPageSize() - this.getSize() >= 0);
            Preconditions.checkState(pager.getPageSize() - newPage.getSize() >= 0);
        }

        synchronized private void sync() throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(new byte[size]);
            buf.put(meta);
            buf.putInt(rightPageNo);
            buf.putInt(map.size());

            for (byte[] key : map.keySet()) {
                buf.putInt(key.length);
                buf.put(key);
                byte[] value = map.get(key);
                if (isLeaf) {
                    buf.putInt(value.length);
                }
                buf.put(value);
            }

            page.updateContent(buf.array());

            SkipList.this.pager.sync(page);
        }
    }

    @Builder
    public SkipList(@NonNull Path path, @NonNull Pager pager) throws IOException {
        this.path = path;
        this.pager = pager;
        metaFilePath = path.resolve(META_FILE_NAME);
        try (SeekableByteChannel chn = Files.newByteChannel(metaFilePath, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocate(4);
            int read = 0;
            while (read != -1 && dst.hasRemaining()) {
                read = chn.read(dst);
            }
            dst.flip();
            height = dst.getInt();
            Preconditions.checkState(height > 0);
            Preconditions.checkState(height <= 0x0f);

            startPages = new HashMap<>(height);

            int lv = 0;
            while (lv < height) {
                read = 0;
                dst.clear();
                while (read != -1 && dst.hasRemaining()) {
                    read = chn.read(dst);
                }
                dst.flip();
                startPages.put(lv, dst.getInt());
                lv++;
            }
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException, ExecutionException {
        try {
            readWriteLock.writeLock().lock();
            int size = HEAD_SIZE_IN_PAGE + 4 + key.length + 4 + value.length;
            if (size > pager.getPageSize()) {
                throw new KeyValueTooLongException("Can't put the key and value in one page.");
            }

            SkipListPage page = findLeafSkipListPageForKey(key);
            page.put(key, value);
            if (checkAfterPut) {
                byte[] v2 = get(key);
                if (!Arrays.equals(value, v2)) {
                    throw new IllegalStateException("get value of key is " + Arrays.toString(v2));
                }
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException, ExecutionException {
        try {
            readWriteLock.readLock().lock();
            SkipListPage page = findLeafSkipListPageForKey(key);
            while (page != null) {
                byte[] v = page.get(key);
                if (v != null) {
                    return v;
                }
                if (page.rightPageNo != -1) {
                    page = skipListPages.get(page.rightPageNo);
                } else {
                    page = null;
                }
            }
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public List<byte[]> range(byte[] min, byte[] max) throws IOException, ExecutionException {
        if (bytesComparator.compare(min, max) >= 0) {
            return Collections.emptyList();
        }
        try {
            readWriteLock.readLock().lock();
            SkipListPage current = findLeafSkipListPageForKey(min);
            List<byte[]> r = new ArrayList<>();
            while (current != null) {
                if (current.getMap().isEmpty() && current.getRightPageNo() != -1) {
                    current = skipListPages.get(current.getRightPageNo());
                    continue;
                }
                if (current.getMap().isEmpty()) {
                    return r;
                }
                if (bytesComparator.compare(max, current.getMap().firstKey()) < 0) {
                    return r;
                }
                r.addAll(current.getMap().subMap(min, max).values());
                if (current.getRightPageNo() != -1) {
                    current = skipListPages.get(current.getRightPageNo());
                } else {
                    current = null;
                }
            }
            return r;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public boolean del(byte[] key) throws IOException, ExecutionException {
        try {
            boolean d = false;
            readWriteLock.writeLock().lock();
            SkipListPage current = findLeafSkipListPageForKey(key);
            if (current.del(key)) {
                d = true;
            }

            for (int lv = height - 1; lv > 0; lv--) {
                SkipListPage p = findSkipListPageForKey(key, lv);
                if (p != null) {
                    p.del(key);
                }
            }
            return d;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private SkipListPage findLeafSkipListPageForKey(byte[] key) throws IOException, ExecutionException {
        SkipListPage p = findSkipListPageForKey(key, 0);
        if (!p.isLeaf) {
            throw new IllegalStateException(p.toString());
        }
        return p;
    }

    private SkipListPage findSkipListPageForKey(byte[] key, int lv) throws IOException, ExecutionException {
        Preconditions.checkArgument(lv >= 0);
        Preconditions.checkArgument(lv < SkipList.this.height);
        int h = height - 1;
        SkipListPage start = null;
        while (h >= lv) {
            if (start == null) {
                start = skipListPages.get(startPages.get(h));
            }
            SkipListPage slice = floorPage(start, key);

            if (h == lv) {
                return slice == null ? start : slice;
            }
            if (slice != null) {
                if (slice.isLeaf) {
                    throw new IllegalStateException();
                }
                Map.Entry<byte[], byte[]> e = slice.map.floorEntry(key);
                int pn = ByteBuffer.wrap(e.getValue()).getInt();
                start = skipListPages.get(pn);
            } else {
                start = null;
            }

            h--;
        }

        return start;
    }

    /**
     * 从指定page开始查找拥有最后一个小于等于待查找值的key所在的page
     *
     * @param start
     * @param key
     * @return skip list page页面
     */
    private SkipListPage floorPage(SkipListPage start, byte[] key) throws ExecutionException, IOException {
        SkipListPage current = start;
        SkipListPage floor = null;
        while (current != null) {
            byte[] floorKey = current.map.floorKey(key);
            if (floorKey != null) {
                // 在本页发现小于key的记录，但还需检查后续页。
                floor = current;
            }

            if (current.getRightPageNo() < 0) {
                return floor;
            }

            int pn = current.getRightPageNo();
            SkipListPage pre = current;
            current = skipListPages.get(pn);
            // 在此回收page
//            if (current.map.isEmpty() && floor != null && !startPages.containsKey(current.getPage().getNo())) {
//                pre.setRightPageNo(current.getRightPageNo());
//                pre.sync();
//                pager.recycle(current.getPage());
//                skipListPages.invalidate(current.getPage().getNo());
//                current = pre;
//            }
        }
        return floor;
    }

    private Page allocatePage(int level) throws IOException {
        Preconditions.checkArgument(level < 0x0f);
        Page page = pager.allocate();
        // init height
        page.replaceBytes(0, 1, ByteBuffer.allocate(1).put((byte) level).array());
        // init right page number
        page.replaceBytes(1, 5, ByteBuffer.allocate(4).putInt(-1).array());
        // init number of keys
        page.replaceBytes(5, 9, ByteBuffer.allocate(4).putInt(0).array());
        pager.sync(page);
        return page;
    }
}
