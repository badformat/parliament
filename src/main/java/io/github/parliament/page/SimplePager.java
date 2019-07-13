package io.github.parliament.page;

import com.google.common.base.Preconditions;
import io.github.parliament.DuplicateKeyException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 从disk一次读取大块数据（allocate），减少io请求。
 *
 * @author zy
 */
@EqualsAndHashCode
public class SimplePager {
    private static final int K = 1024;
    private Path space;
    private int pageSize = 64 * K;
    private ConcurrentHashMap<Integer, SimplePage> pages = new ConcurrentHashMap<>();

    @Builder
    private SimplePager(@NonNull String path, Integer pageSize) throws IOException {
        this.space = Paths.get(path);
        if (pageSize != null) {
            this.pageSize = pageSize;
        }

        if (!Files.exists(space)) {
            Files.createDirectories(space);
        }
    }

    public synchronized void insert(byte[] key, byte[] value) throws IOException, DuplicateKeyException {
        Preconditions.checkState(key.length <= Short.MAX_VALUE, "key is too long.");
        Preconditions.checkState(value.length <= Short.MAX_VALUE, "value is too long.");

        SimplePage page = page(0);

        page.insert(key, value);
        sync(page);
    }

    public synchronized boolean containsKey(byte[] key) throws IOException {
        SimplePage page = page(0);
        return page.containsKey(key);
    }

    public synchronized byte[] get(byte[] key) throws IOException {
        SimplePage page = page(0);
        return page.get(key);
    }

    public synchronized byte[] get(String key) throws IOException {
        return get(key.getBytes());
    }

    public boolean remove(byte[] key) throws IOException {
        SimplePage page = page(0);
        if (page.remove(key)) {
            sync(page);
            return true;
        }
        return false;
    }

    public byte[] range(byte[] begin, byte[] end, int limit) {
        return null;
    }

    private SimplePage page(int i) throws IOException {
        if (pages.containsKey(i)) {
            return pages.get(i);
        }

        Path heap = space.resolve("heap" + i);
        if (!Files.exists(heap)) {
            Files.createFile(heap);
        }

        try (SeekableByteChannel chn = Files.newByteChannel(heap, StandardOpenOption.READ)) {
            ByteBuffer dst = ByteBuffer.allocate(pageSize);
            int read = 0;
            do {
                read = chn.read(dst);
            } while (read != -1 && read != 0);

            dst.flip();
            SimplePage page = new SimplePage(dst, pageSize);
            pages.put(0, page);
            return page;
        }
    }

    public void sync(SimplePage page) throws IOException {
        Path heap = space.resolve("heap0");
        try (SeekableByteChannel chn = Files.newByteChannel(heap, StandardOpenOption.WRITE)) {
            page.write(chn);
        }
    }
}