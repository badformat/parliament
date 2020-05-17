package io.github.parliament;

import com.google.common.primitives.UnsignedBytes;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

/**
 * @author zy
 **/
public class LevelDB implements Persistence {
    private DB db;

    public static LevelDB open(Path path) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        DB db = factory.open(new File(path.toString()), options);
        return new LevelDB(db);
    }

    public LevelDB(DB db) {
        this.db = db;
    }

    @Override
    public void put(byte[] bytes, byte[] array) {
        db.put(bytes, array);
    }

    @Override
    public byte[] get(byte[] key) {
        return db.get(key);
    }

    @Override
    public boolean del(byte[] key) {
        boolean exists = db.get(key) != null;
        db.delete(key);
        return exists;
    }

    @Override
    public List<byte[]> range(byte[] min, byte[] max) throws IOException, ExecutionException {
        List<byte[]> r = new ArrayList<>();
        DBIterator iterator = db.iterator();
        iterator.seek(min);
        Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();
        for (; iterator.hasNext(); iterator.next()) {
            if (bytesComparator.compare(iterator.peekNext().getKey(), max) <= 0) {
                r.add(iterator.peekNext().getValue());
            }
        }
        return r;
    }
}
