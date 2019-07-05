package io.github.parliament.skiplist;

import io.github.parliament.page.Pager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class SkipList {
    public static final String META_FILE_NAME = "skiplist.mf";
    private Pager pager;

    public void createMetaInf(String dir, int level) throws IOException {
        Path metaFilePath = Paths.get(dir, META_FILE_NAME);
        Files.newByteChannel(metaFilePath, StandardOpenOption.CREATE_NEW);
    }

    public void put(byte[] key, byte[] value) {

    }

    public void setPager(Pager pager) {
        this.pager = pager;
    }

    public int height() {
        return 3;
    }

    public Node getLevelFirstNode(int height) {
        // TODO Auto-generated method stub
        return null;
    }
}
