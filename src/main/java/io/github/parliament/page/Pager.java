package io.github.parliament.page;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Pager {
    private Path path;
    private WriteAheadLog wal;

    public Pager(String dir) {
        path = Paths.get(dir);
        wal = new WriteAheadLog(dir);
    }

    public void init() throws IOException {
        if (Files.exists(path.resolve("page_seq"))) {
            return;
        }
        Files.createFile(path.resolve("page_seq"));
    }

    public Page page(int pageNo) {
        return null;
    }

    Page allocate() {
        int pageNo = seq();
        try {
            return null;
        } finally {

        }
    }

    private int seq() {
        return 0;
    }
}
