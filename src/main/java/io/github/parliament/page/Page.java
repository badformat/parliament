package io.github.parliament.page;

import io.github.parliament.DuplicateKeyException;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zy
 */
@EqualsAndHashCode
class Page {
    private static final int OFFSET = 4;
    private static final int PRE_OFFSET = 4;
    private static final int NEXT_OFFSET = 4;
    private static final int RECORD_NO = 2;
    private static final int RECORD_HEAD = 3;

    private List<Record> records = new ArrayList<>();
    private int size;

    Page(ByteBuffer buffer, int size) {
        this.size = size;
        load(buffer);
    }

    private void load(ByteBuffer buffer) {
        if (buffer.limit() < 2) {
            return;
        }
        short recsno = buffer.getShort();
        int offset = 0;
        for (int i = 0; i < recsno; i++) {
            short rl = buffer.getShort();
            if (rl < 0) {
                throw new IllegalStateException();
            }
            byte stat = buffer.get();
            buffer.mark();

            buffer.position(RECORD_NO + recsno * RECORD_HEAD + offset);
            byte[] dst = new byte[rl];
            buffer.get(dst, 0, rl);
            Record record = new Record(stat, dst);
            records.add(record);
            offset += rl;

            buffer.reset();
        }
    }

    void insert(byte[] key, byte[] value) throws DuplicateKeyException {
        for (Record record : records) {
            if (Arrays.equals(record.key(), key)) {
                throw new DuplicateKeyException();
            }
        }
        records.add(new Record((byte) 0, key, value));
    }

    boolean containsKey(byte[] key) {
        for (Record record : records) {
            ByteBuffer buffer = ByteBuffer.wrap(record.bytes());
            int s = buffer.getShort();
            if (s != key.length) {
                continue;
            }

            byte[] k = new byte[s];
            buffer.get(k, 0, s);
            if (Arrays.equals(key, k)) {
                return true;
            }
        }
        return false;
    }

    byte[] get(byte[] key) {
        for (Record record : records) {
            ByteBuffer buffer = ByteBuffer.wrap(record.bytes());
            int s = buffer.getShort();
            if (s != key.length) {
                continue;
            }

            byte[] k = new byte[s];
            buffer.get(k, 0, s);
            if (Arrays.equals(key, k)) {
                s = buffer.getShort();
                byte[] value = new byte[s];
                buffer.get(value, 0, s);
                return value;
            }
        }
        return null;
    }

    void write(SeekableByteChannel chn) throws IOException {
        ByteBuffer src = ByteBuffer.allocate(size);
        src.putShort((short) records.size());
        for (Record rec : records) {
            src.putShort(rec.length());
            src.put(rec.stat());
        }
        for (Record rec : records) {
            src.put(rec.bytes(), 0, rec.length());
        }

        src.flip();

        while (src.hasRemaining()) {
            chn.write(src);
        }
    }

    boolean remove(byte[] key) {
        Record remove = null;
        for (Record record : records) {
            ByteBuffer buffer = ByteBuffer.wrap(record.bytes());
            int s = buffer.getShort();
            if (s != key.length) {
                continue;
            }
            byte[] k = new byte[s];
            buffer.get(k, 0, s);
            if (Arrays.equals(key, k)) {
                remove = record;
            }
        }
        return records.remove(remove);
    }

}