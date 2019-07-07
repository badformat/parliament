package io.github.parliament.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.google.common.base.Preconditions;

import lombok.Getter;

public class Pager {
    public static final int MAX_HEAP_SIZE = 4 * 1024 * 1024 * 1024;
    public static final String PAGE_SEQ_FILENAME = "page_seq";
    public static final String METAINF_FILENAME = "metainf";
    public static final String HEAP_FILENAME_PREFIX = "heap";
    static final int PAGE_HEAD_SIZE = 8;

    private Path path;
    @Getter
    private int heapSize;
    @Getter
    private int pageSize;
    private byte[][] heads;

    public static void init(Path path, int heapSize, int pageSize) throws IOException {
        if (!Files.exists(path)) {
            Files.createDirectory(path);
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

        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(HEAP_FILENAME_PREFIX + "0"),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (heads > 0) {
                ByteBuffer src = ByteBuffer.allocate(4).putInt(-1).flip();
                while (src.hasRemaining()) {
                    chn.write(src);
                }
                src.flip();
                while (src.hasRemaining()) {
                    chn.write(src);
                }
                heads--;
            }
        }
    }

    static int maxPagesInHeap(int heapSize, int pageSize) {
        return heapSize / (PAGE_HEAD_SIZE + pageSize);
    }

    public Pager(String dir) throws IOException {
        path = Paths.get(dir);
        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(METAINF_FILENAME), StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocate(8);
            int read = 0;
            while (read != -1 && dst.hasRemaining()) {
                read = chn.read(dst);
            }
            dst.flip();
            heapSize = dst.getInt();
            pageSize = dst.getInt();
            heads = new byte[Pager.maxPagesInHeap(heapSize, pageSize)][Pager.PAGE_HEAD_SIZE];
        }
    }

    public Page page(int pageNo) {
        return null;
    }

    Page allocate() throws IOException {
        int pageNo = getAndIncreament();
        try {
            return null;
        } finally {

        }
    }

    private synchronized int getAndIncreament() throws IOException {
        try (SeekableByteChannel chn = Files.newByteChannel(path.resolve(PAGE_SEQ_FILENAME),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer dst = ByteBuffer.allocate(4);
            chn.read(dst);
            dst.flip();
            int i = dst.getInt();
            dst.clear();
            dst.putInt(i + 1);
            while (dst.hasRemaining()) {
                chn.write(dst);
            }
            return i;
        }
    }

    public Heads getHeads() {
        // TODO Auto-generated method stub
        return null;
    }
}
