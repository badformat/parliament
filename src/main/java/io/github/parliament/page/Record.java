package io.github.parliament.page;

import java.nio.ByteBuffer;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 *
 * @author zy
 */
@EqualsAndHashCode
@ToString
class Record {
    private byte   stat;
    private byte[] key;
    private byte[] value;
    private byte[] bytes;

    Record(byte stat, byte[] key, byte[] value) {
        int len = 2 + key.length + 2 + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        bytes = new byte[len];

        buffer.putShort((short) key.length);
        buffer.put(key, 0, key.length);
        buffer.putShort((short) value.length);
        buffer.put(value, 0, value.length);

        buffer.flip();
        buffer.get(bytes);

        this.stat = stat;
        this.key = key;
        this.value = value;
    }

    Record(byte stat, byte[] content) {
        this.stat = stat;
        this.bytes = content;
        ByteBuffer buf = ByteBuffer.wrap(content);
        short kl = buf.getShort();
        key = new byte[kl];
        buf.get(key);
        short vl = buf.getShort();
        value = new byte[vl];
        buf.get(value);
    }

    byte[] key() {
        return key;
    }

    byte[] value() {
        return value;
    }

    byte stat() {
        return stat;
    }

    short length() {
        return (short) bytes.length;
    }

    byte[] bytes() {
        return bytes;
    }
}