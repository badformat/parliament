package io.github.parliament;

import io.github.parliament.page.Pager;
import lombok.Builder;
import lombok.NonNull;

import java.io.IOException;
import java.nio.file.Path;

public class PagePersistence implements Persistence {
    Pager pager;

    @Builder
    private PagePersistence(@NonNull Path path) throws IOException {
        pager = Pager.builder().path(path.toString()).build();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        synchronized ((pager)) {
            if (pager.containsKey(key)) {
                pager.remove(key);
            }
            pager.insert(key, value);
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return pager.get(key);
    }

    @Override
    public boolean remove(byte[] key) throws IOException {
        synchronized (pager) {
            return pager.remove(key);
        }
    }
}
