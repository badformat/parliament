package io.github.parliament.skiplist;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
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
    private int[] startPages;
    @Getter(AccessLevel.PACKAGE)
    private final LoadingCache<Integer, SkipListPage> skipListPages = CacheBuilder
            .newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .removalListener((RemovalListener<Integer, SkipListPage>) notification -> {
                try {
                    notification.getValue().sync();
                } catch (IOException e) {
                    logger.error("sync skip list page failed.", e);
                }
            })
            .build(new CacheLoader<>() {
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
    private boolean getAfterPut = false;

    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public static void init(Path path, int height, Pager pager) throws IOException {
        Path metaFilePath = path.resolve(META_FILE_NAME);
        if (Files.exists(metaFilePath)) {
            return;
        }
        Preconditions.checkArgument(height > 0);
        Preconditions.checkArgument(height <= 0x0f);

        try (SeekableByteChannel chn = Files.newByteChannel(metaFilePath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer src = ByteBuffer.allocate(4).putInt(height).flip();
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

                src.clear().putInt(page.getNo()).flip();
                while (src.hasRemaining()) {
                    chn.write(src);
                }
                level++;
            }
        }
    }

    @EqualsAndHashCode
    @ToString(exclude = "superPage")
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
        private volatile int rightPage;
        @Getter
        private volatile int keys;
        @Getter
        private volatile boolean isLeaf;
        @Getter
        private ConcurrentNavigableMap<byte[], byte[]> map;
        @Getter
        private volatile SkipListPage superPage;
        @Getter
        private volatile boolean dirty = false;

        SkipListPage(Page page) {
            this.page = page;
            ByteBuffer buf = ByteBuffer.wrap(page.getContent());
            meta = buf.get();
            level = meta & 0x0f;
            isLeaf = (level == 0);
            rightPage = buf.getInt();
            keys = buf.getInt();
            size = HEAD_SIZE_IN_PAGE;
            map = new ConcurrentSkipListMap<>(Arrays::compare);
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
            dirty = true;
        }

        void put(byte[] key, byte[] value) throws IOException, ExecutionException {
            int size = 4 + key.length + value.length;
            if (isLeaf) {
                // value length field.
                size += 4;
            }

            if (size > pager.getPageSize()) {
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
            update();
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
            if (superPage == null) {
                superPage = findSkipListPageOfKey(key, level + 1);
            }

            byte[] value = ByteBuffer.allocate(4).putInt(page.getNo()).array();
            superPage.put(key, value);
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
            keys = k;

            int left = pager.getPageSize() - this.getSize();
            if (left < 0) {
                // check left size
                // allocate a new page and insert it into list
                split();
            }
        }

        private void split() throws IOException, ExecutionException {
            Preconditions.checkState(this.keys > 0);
            Preconditions.checkState(!map.isEmpty());
            Page p = SkipList.this.allocatePage(level);
            SkipListPage page = skipListPages.get(p.getNo());

            page.map = new ConcurrentSkipListMap<>(map.tailMap(map.firstKey(), false));
            Preconditions.checkState(!map.isEmpty());
            map = new ConcurrentSkipListMap<>(map.headMap(map.firstKey(), true));

            int right = this.rightPage;
            this.rightPage = page.getPage().getNo();
            page.rightPage = right;

            this.update();
            page.update();

            Preconditions.checkState(pager.getPageSize() - this.getSize() >= 0);
            Preconditions.checkState(pager.getPageSize() - page.getSize() >= 0);
        }

        synchronized void sync() throws IOException {
            if (!isDirty()) {
                return;
            }
            ByteBuffer buf = ByteBuffer.wrap(new byte[size]);
            buf.put(meta);
            buf.putInt(rightPage);
            buf.putInt(keys);

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
            dirty = false;
        }

        void setSuperPage(SkipListPage superPage) {
            Preconditions.checkArgument(this != superPage);
            if (superPage != null) {
                Preconditions.checkArgument(this.getPage().getNo() != superPage.getPage().getNo());
            }

            this.superPage = superPage;
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
            height = dst.flip().getInt();
            Preconditions.checkState(height > 0);
            Preconditions.checkState(height <= 0x0f);

            startPages = new int[height];

            int lv = 0;
            while (lv < height) {
                read = 0;
                dst.clear();
                while (read != -1 && dst.hasRemaining()) {
                    read = chn.read(dst);
                }
                startPages[lv] = dst.flip().getInt();
                lv++;
            }
        }
    }

    public void put(byte[] key, byte[] value) throws IOException, ExecutionException {
        try {
            readWriteLock.writeLock().lock();
            int size = HEAD_SIZE_IN_PAGE + 4 + key.length + 4 + value.length;
            if (size > pager.getPageSize()) {
                throw new KeyValueTooLongException("Can't put the key and value in one page.");
            }

            SkipListPage page = findLeafSkipListPageOfKey(key);
            page.put(key, value);
            if (getAfterPut) {
                byte[] v2 = get(key);
                if (!Arrays.equals(value, v2)) {
                    throw new IllegalStateException("get value of key is " + v2);
                }
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }


    public byte[] get(byte[] key) throws IOException, ExecutionException {
        try {
            readWriteLock.readLock().lock();
            SkipListPage page = findLeafSkipListPageOfKey(key);
            while (page != null) {
                byte[] v = page.get(key);
                if (v != null) {
                    return v;
                }
                if (page.rightPage != -1) {
                    page = skipListPages.get(page.rightPage);
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
        if (Arrays.compare(min, max) >= 0) {
            return Collections.emptyList();
        }
        try {
            readWriteLock.readLock().lock();
            SkipListPage current = findLeafSkipListPageOfKey(min);
            List<byte[]> r = new ArrayList<>();
            while (current != null) {
                if (current.getMap().isEmpty()) {
                    return r;
                }
                if (Arrays.compare(max, current.getMap().firstKey()) < 0) {
                    return r;
                }
                r.addAll(current.getMap().subMap(min, max).values());
                if (current.getRightPage() != -1) {
                    current = skipListPages.get(current.getRightPage());
                } else {
                    current = null;
                }
            }
            return r;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public boolean del(byte[] key) throws IOException, ExecutionException {
        try {
            boolean d = false;
            readWriteLock.writeLock().lock();
            SkipListPage current = findLeafSkipListPageOfKey(key);
            if (current.del(key)) {
                d = true;
            }
            SkipListPage superPage = current.getSuperPage();
            if (superPage != null) {
                superPage.del(key);
            }
            return d;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private SkipListPage findLeafSkipListPageOfKey(byte[] key) throws IOException, ExecutionException {
        return findSkipListPageOfKey(key, 0);
    }

    private SkipListPage findSkipListPageOfKey(byte[] key, int lv) throws IOException, ExecutionException {
        Preconditions.checkArgument(lv >= 0);
        Preconditions.checkArgument(lv < SkipList.this.height);
        int cursor = height - 1;
        SkipListPage start = null;
        SkipListPage up = null;
        while (cursor >= lv) {
            up = start;
            if (start == null) {
                start = skipListPages.get(startPages[cursor]);
            }
            SkipListPage slice = floorPage(start, key);

            if (cursor == lv) {
                return slice == null ? start : slice;
            }
            if (slice != null) {
                Map.Entry<byte[], byte[]> e = slice.map.floorEntry(key);
                int pn = ByteBuffer.wrap(e.getValue()).getInt();
                start = skipListPages.get(pn);
                start.setSuperPage(up);
            } else {
                start = null;
            }

            cursor--;
        }

        return start;
    }

    /**
     * 从指定page开始查找最后一个小于等于lookup的key对应的page
     *
     * @param start
     * @param key
     * @return
     * @throws IOException
     */
    private SkipListPage floorPage(SkipListPage start, byte[] key) throws IOException, ExecutionException {
        SkipListPage current = start;
        SkipListPage found = null;
        while (current != null) {
            Map.Entry<byte[], byte[]> entry = current.map.floorEntry(key);
            if (entry != null) {
                found = current;
            }

            if (current.getRightPage() > 0) {
                int pn = current.getRightPage();
                SkipListPage right = skipListPages.get(pn);
                if (right.map.isEmpty()) {
                    current.rightPage = right.rightPage;
                    current.sync();
                } else {
                    current = right;
                }

            } else {
                current = null;
            }
        }
        return found;
    }

    private Page allocatePage(int level) throws IOException {
        Page page = pager.allocate();
        Preconditions.checkArgument(level < 0x0f);
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
