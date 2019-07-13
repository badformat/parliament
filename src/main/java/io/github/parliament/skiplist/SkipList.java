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

public class SkipList {
    static final String META_FILE_NAME = "skiplist.mf";
    static final int HEAD_SIZE_IN_PAGE = 1 + 4 + 4;

    private Pager pager;
    @Getter
    private Path path;
    private Path metaFilePath;
    @Getter
    private int height;
    @Getter
    private int[] startPages;
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
                // init right getOrCreatePage
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
        private SkipListPage upLevelPage;

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
            if (page.getUpLevelPage() != null) {
                upLevelPage = skipListPages.computeIfAbsent(page.getUpLevelPage().getNo(),
                        (k) -> new SkipListPage(page.getUpLevelPage()));
            }
        }

        void put(byte[] key, byte[] value) throws IOException {
            int size = 4 + key.length + value.length;
            if (isLeaf) {
                // value length field.
                size += 4;
            }

            if (size > pager.getPageSize()) {
                throw new KeyValueTooLongException("Can't put the key and value in one getOrCreatePage.");
            }

            SkipListPage sp = this;
            int left = pager.getPageSize() - sp.getSize();
            synchronized (sp.getPage()) {
                // check left size
                while (left < size) {
                    // allocate a new getOrCreatePage and insert it into list
                    sp = sp.split();
                    left = pager.getPageSize() - sp.getSize();
                }
            }

            synchronized (sp.getPage()) {
                int cur = sp.keys;
                if (cur == 0) {
                    sp.head = sp.new Node(key, value);
                    sp.update();
                    return;
                }
                SkipListPage.Node node = sp.getHead();
                SkipListPage.Node pre = node;
                while (cur > 0) {
                    // get key
                    int c = Arrays.compare(key, node.getKey());
                    if (c < 0) {
                        node = node.getNext();
                        pre = node;
                    } else if (c > 0) {
                        // to insert
                        SkipListPage.Node newNode = sp.new Node(key, value);
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
                throw new IllegalStateException();
            }
        }

        void promo(byte[] key) throws IOException {
            Preconditions.checkState(level < SkipList.this.height);
            if (level == SkipList.this.height - 1) {
                return;
            }
            if (upLevelPage == null) {
                Page np = firstPageOfLevel(level + 1);
                upLevelPage = skipListPages.computeIfAbsent(np.getNo(),
                        (k) -> new SkipListPage(np));
            }

            byte[] value = ByteBuffer.allocate(4).putInt(page.getNo()).array();
            upLevelPage.put(key, value);
        }

        private SkipListPage split() throws IOException {
            int k = this.keys / 2;
            Node tail = this.head;
            while (k > 0) {
                tail = tail.next;
                k--;
            }

            Page newPage = SkipList.this.allocatePage(level);
            SkipListPage sp = skipListPages.computeIfAbsent(newPage.getNo(), (pn) -> new SkipListPage(newPage));

            if (this.keys / 2 > 0) {
                sp.head = tail;
            }
            sp.rightPage = this.rightPage;
            this.rightPage = sp.getPage().getNo();

            this.update();
            sp.update();
            return sp;
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
        return pager.getOrCreatePage(startPages[level]);
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
            throw new KeyValueTooLongException("Can't put the key and value in one getOrCreatePage.");
        }

        final Page page = findLeafPageOfKey(key);
        SkipListPage sp = skipListPages.computeIfAbsent(page.getNo(), (k) -> new SkipListPage(page));
        sp.put(key, value);
    }

    void sync() throws IOException {
        for (SkipListPage page : skipListPages.values()) {
            page.sync();
        }
    }

    Page findPageOfKeyInLevel(int lv, byte[] key) throws IOException {
        Preconditions.checkArgument(lv >= 0);
        Preconditions.checkArgument(lv < SkipList.this.height);
        int cur = height - 1;
        Page page = null;
        while (cur > lv) {
            if (page == null) {
                page = pager.getOrCreatePage(startPages[cur]);
            }
            cur--;
            byte[] v = findValueJustLessThan(page, key);
            if (v == null) {
                page = null;
            } else {
                Page upLevelPage = page;
                page = pager.getOrCreatePage(ByteBuffer.wrap(v).getInt());
                page.setUpLevelPage(upLevelPage);
            }
        }

        if (page == null) {
            page = pager.getOrCreatePage(startPages[lv]);
        }

        return findPageContainsKey(page, key);
    }

    Page findLeafPageOfKey(byte[] key) throws IOException {
        return findPageOfKeyInLevel(0, key);
    }

    /**
     * 从指定page开始查找最后一个小于up的key对应的值
     *
     * @param page
     * @param up
     * @return
     * @throws IOException
     */
    private byte[] findValueJustLessThan(Page page, byte[] up) throws IOException {
        while (page != null) {
            final Page p = page;
            SkipListPage sp = skipListPages.computeIfAbsent(p.getNo(), (k) -> new SkipListPage(p));
            synchronized (sp.getPage()) {
                SkipListPage.Node node = sp.getHead();
                while (node != null) {
                    if (Arrays.compare(up, node.getKey()) > 0) {
                        node = node.getNext();
                    } else {
                        return node.getValue();
                    }
                }

                if (sp.getRightPage() > 0) {
                    page = pager.getOrCreatePage(sp.getRightPage());
                } else {
                    page = null;
                }
            }
        }
        return null;
    }

    private Page findPageContainsKey(Page start, byte[] key) throws IOException {
        Page page = start;
        while (page != null) {
            final Page p = page;
            SkipListPage sp = skipListPages.computeIfAbsent(p.getNo(), (k) -> new SkipListPage(p));
            synchronized (sp.getPage()) {
                SkipListPage.Node node = sp.getHead();
                while (node != null) {
                    if (Arrays.compare(key, node.getKey()) > 0) {
                        node = node.getNext();
                    } else {
                        return sp.getPage();
                    }
                }

                if (sp.getRightPage() > 0) {
                    page = pager.getOrCreatePage(sp.getRightPage());
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
        // init right getOrCreatePage number
        page.replaceBytes(1, 5, ByteBuffer.allocate(4).putInt(-1).array());
        // init number of keys
        page.replaceBytes(5, 9, ByteBuffer.allocate(4).putInt(0).array());
        pager.sync(page);
        return page;
    }


}
