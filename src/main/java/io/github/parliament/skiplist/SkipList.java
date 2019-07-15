package io.github.parliament.skiplist;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import io.github.parliament.page.Page;
import io.github.parliament.page.Pager;
import lombok.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

public class SkipList {
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
    private ConcurrentMap<Integer, SkipListPage> skipListPages = new MapMaker().makeMap();

    public static void init(Path path, int height, Pager pager) throws IOException {
        Path metaFilePath = path.resolve(META_FILE_NAME);
        Preconditions.checkState(!Files.exists(metaFilePath));
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
    @ToString
    class SkipListPage {
        @Getter
        private Page page;
        @Getter
        private byte meta;
        @Getter
        private int level;
        @Getter
        private int size;
        @Getter
        private int rightPage;
        @Getter
        private int keys;
        @Getter
        private boolean isLeaf;
        @Getter
        private Node head;
        @Getter
        private SkipListPage superPage;

        SkipListPage(Page page) {
            this.page = page;
            ByteBuffer buf = ByteBuffer.wrap(page.getContent());
            meta = buf.get();
            level = meta & 0x0f;
            isLeaf = (level == 0);
            rightPage = buf.getInt();
            keys = buf.getInt();
            size = HEAD_SIZE_IN_PAGE;
            Node pre = null;
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

                    if (head == null) {
                        head = new Node(key, value);
                        pre = head;
                    } else {
                        pre.next = new Node(key, value);
                        pre = pre.next;
                    }
                    cur--;
                }
            }
        }

        void put(byte[] key, byte[] value) throws IOException {
            int size = 4 + key.length + value.length;
            if (isLeaf) {
                // value length field.
                size += 4;
            }

            if (size > pager.getPageSize()) {
                throw new KeyValueTooLongException("Can't put the key and value in one page.");
            }

            SkipListPage page = this;
            int left = pager.getPageSize() - this.getSize();
            if (size > left) {
                synchronized (page.getPage()) {
                    // check left size
                    // allocate a new page and insert it into list
                    page = page.split();
                    left = pager.getPageSize() - page.getSize();
                    Preconditions.checkState(left >= size);
                }
            }

            synchronized (page.getPage()) {
                int cur = page.keys;
                if (cur == 0) {
                    Preconditions.checkState(page.head == null);
                    page.head = page.new Node(key, value);
                    page.update();
                    return;
                }
                SkipListPage.Node node = page.getHead();
                SkipListPage.Node pre = node;
                while (cur > 0) {
                    // get key
                    int c = Arrays.compare(key, node.getKey());
                    if (c < 0) {
                        node = node.getNext();
                        pre = node;
                    } else if (c > 0) {
                        // to insert
                        SkipListPage.Node newNode = page.new Node(key, value);
                        pre.append(newNode);
                        update();
                        return;
                    } else {
                        // to replace
                        node.updateValue(value);
                        update();
                        return;
                    }
                    cur--;
                }

                SkipListPage.Node newNode = page.new Node(key, value);

                if (pre != null) {
                    pre.append(newNode);
                } else {
                    this.head = newNode;
                }
                page.update();
            }
        }

        void promo(byte[] key) throws IOException {
            Preconditions.checkState(level < SkipList.this.height);
            if (level == SkipList.this.height - 1) {
                return;
            }
            if (superPage == null) {
                superPage = findSkipListPageOfKey(key, level + 1);
            }

            byte[] value = ByteBuffer.allocate(4).putInt(page.getNo()).array();
            superPage.put(key, value);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (random.nextBoolean()) {
                superPage.promo(key);
            }
        }

        SkipListPage split() throws IOException {
            Preconditions.checkState(this.keys > 0);
            Page p = SkipList.this.allocatePage(level);
            SkipListPage page = skipListPages.computeIfAbsent(p.getNo(), (pn) -> new SkipListPage(p));

            int right = this.rightPage;
            page.rightPage = right;
            this.rightPage = page.getPage().getNo();

            this.update();
            page.update();
            return page;
        }

        private void update() {
            int s = HEAD_SIZE_IN_PAGE;
            int k = 0;

            Node node = head;
            while (node != null) {
                k++;
                s += node.key.length;
                s += 4;
                s += node.value.length;
                if (isLeaf) {
                    s += 4;
                }
                node = node.next;
            }
            size = s;
            keys = k;
        }

        private void sync() throws IOException {
            update();
            ByteBuffer buf = ByteBuffer.wrap(new byte[size]);
            buf.put(meta);
            buf.putInt(rightPage);
            buf.putInt(keys);
            Node node = head;
            while (node != null) {
                buf.putInt(node.key.length);
                buf.put(node.key);
                if (isLeaf) {
                    buf.putInt(node.value.length);
                }
                buf.put(node.value);
                node = node.next;
            }

            page.updateContent(buf.array());

            SkipList.this.pager.sync(page);
        }

        void setSuperPage(SkipListPage superPage) {
            this.superPage = superPage;
        }

        class Node {
            @Getter
            private byte[] key;
            @Getter
            private byte[] value;
            @Getter
            @Setter(AccessLevel.PRIVATE)
            private Node next;

            private Node(byte[] key, byte[] value) {
                this.key = key;
                this.value = value;
            }

            private void append(Node newNode) {
                newNode.next = this.next;
                this.next = newNode;
            }

            private void updateValue(byte[] value) {
                this.value = value;
            }
        }

    }

    private Page firstPageOfLevel(int level) throws IOException {
        return pager.page(startPages[level]);
    }

    @Builder
    private SkipList(@NonNull Path path, @NonNull Pager pager) throws IOException {
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

    public void put(byte[] key, byte[] value) throws IOException {
        int size = HEAD_SIZE_IN_PAGE + 4 + key.length + 4 + value.length;
        if (size > pager.getPageSize()) {
            throw new KeyValueTooLongException("Can't put the key and value in one page.");
        }

        SkipListPage page = findLeafSkipListPageOfKey(key);
        page.put(key, value);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (random.nextBoolean()) {
            page.promo(key);
        }
    }

    void sync() throws IOException {
        for (SkipListPage page : skipListPages.values()) {
            page.sync();
        }
    }

    SkipListPage findLeafSkipListPageOfKey(byte[] key) throws IOException {
        return findSkipListPageOfKey(key, 0);
    }

    SkipListPage findSkipListPageOfKey(byte[] key, int lv) throws IOException {
        Preconditions.checkArgument(lv >= 0);
        Preconditions.checkArgument(lv < SkipList.this.height);
        int cur = height - 1;
        SkipListPage page = null;
        while (cur > lv) {
            if (page == null) {
                page = skipListPages.get(startPages[cur]);
                if (page == null) {
                    Page p = pager.page(startPages[cur]);
                    page = addSkipListPages(p);
                }
            }
            cur--;
            byte[] v = findValueNotLessThan(page, key);
            if (v == null) {
                page = null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(v);
                int pn = buf.getInt();
                Preconditions.checkState(!buf.hasRemaining());

                SkipListPage superPage = page;
                page = skipListPages.get(pn);
                if (page == null) {
                    Page p = pager.page(pn);
                    page = addSkipListPages(p);
                }
                page.setSuperPage(superPage);
            }
        }

        if (page == null) {
            int pn = startPages[cur];
            page = skipListPages.get(pn);
            if (page == null) {
                Page p = pager.page(pn);
                page = addSkipListPages(p);
            }
        }

        return findMostRightPage(page, key);
    }

    private SkipListPage addSkipListPages(Page p) {
        return skipListPages.computeIfAbsent(p.getNo(), (k) -> new SkipListPage(p));
    }

    /**
     * 从指定page开始查找最后一个小于up的key对应的值
     *
     * @param page
     * @param up
     * @return
     * @throws IOException
     */
    private byte[] findValueNotLessThan(SkipListPage page, byte[] up) throws IOException {
        while (page != null) {
            synchronized (page.getPage()) {
                SkipListPage.Node node = page.getHead();
                SkipListPage.Node last = node;
                while (node != null) {
                    if (Arrays.compare(up, node.getKey()) > 0) {
                        node = node.getNext();
                        last = node;
                    } else {
                        return node.getValue();
                    }
                }

                if (page.getRightPage() > 0) {
                    int pageNo = page.getRightPage();
                    page = skipListPages.computeIfAbsent(page.getRightPage(), (k) -> {
                        try {
                            return new SkipListPage(pager.page(pageNo));
                        } catch (IOException e) {
                            return null;
                        }
                    });
                } else {
                    if (last == null) {
                        return null;
                    }
                    return last.getValue();
                }
            }
        }
        return null;
    }

    private SkipListPage findMostRightPage(SkipListPage start, byte[] key) throws IOException {
        SkipListPage page = start;
        while (page != null) {
            synchronized (page.getPage()) {
                SkipListPage.Node node = page.getHead();
                while (node != null) {
                    if (Arrays.compare(key, node.getKey()) > 0) {
                        node = node.getNext();
                    } else {
                        return page;
                    }
                }

                if (page.getRightPage() > 0) {
                    int pn = page.getRightPage();
                    page = skipListPages.get(pn);
                    if (page == null) {
                        Page p = pager.page(pn);
                        page = addSkipListPages(p);
                    }
                } else {
                    page = null;
                }
            }
        }
        return start;
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
