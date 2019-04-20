package io.github.parliament.resp.reader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespError;
import io.github.parliament.resp.RespInteger;
import io.github.parliament.resp.RespSimpleString;

/**
 *
 * @author zy
 */
public class RespParser {
    private ByteChannel channel;
    private ByteBuffer  bb;

    public static RespParser create(ByteChannel channel) {
        return new RespParser(channel);
    }

    private RespParser(ByteChannel channel) {
        this.channel = channel;
        this.bb = ByteBuffer.allocate(5120);
        bb.flip();
    }

    public RespData get() throws IOException {
        readBytesIfNotHasRemaining();

        byte firstByte = bb.get(bb.position());
        switch (firstByte) {
            case RespArray.firstByte:
                return getAsArray();
            case RespSimpleString.firstByte:
                return getAsSimpleString();
            case RespBulkString.firstByte:
                return getAsBulkString();
            case RespError.firstByte:
                return getAsError();
            case RespInteger.firstByte:
                return getAsInteger();
            default:
                throw new IllegalStateException();
        }
    }

    public RespInteger getAsInteger() throws IOException {
        return RespInteger.with(readStringLine((byte) RespInteger.firstByte));
    }

    public RespError getAsError() throws IOException {
        return RespError.withUTF8(readStringLine((byte) RespError.firstByte));
    }

    public RespBulkString getAsBulkString() throws IOException {
        readBytesIfNotHasRemaining();
        byte firstByte = bb.get();
        Preconditions.checkState(firstByte == RespBulkString.firstByte);

        int len = readLength();
        RespBulkString bs = null;
        if (len != -1) {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                while (len > 0) {
                    readBytesIfNotHasRemaining();
                    os.write(bb.get());
                    len--;
                }

                bs = RespBulkString.with(os.toByteArray());
            }

            readBytesIfNotHasRemaining();
            Preconditions.checkState(bb.get() == '\r');
            readBytesIfNotHasRemaining();
            Preconditions.checkState(bb.get() == '\n');
        } else {
            bs = RespBulkString.nullBulkString();
        }
        return bs;
    }

    public RespSimpleString getAsSimpleString() throws IOException {
        return RespSimpleString.withUTF8(readStringLine((byte) RespSimpleString.firstByte));
    }

    public RespArray getAsArray() throws IOException {
        readBytesIfNotHasRemaining();
        byte firstByte = bb.get();
        Preconditions.checkState(firstByte == RespArray.firstByte);

        int len = readLength();

        if (len == 0) {
            return RespArray.empty();
        }

        List<RespData> datas = new ArrayList<>();
        while (len > 0) {
            datas.add(get());
            len--;
        }

        return RespArray.with(datas);
    }

    private String readStringLine(byte firstByte) throws IOException {
        readBytesIfNotHasRemaining();
        byte fb = bb.get();
        Preconditions.checkState(fb == firstByte);

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            byte[] bytes = readLine(os);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private int readLength() throws IOException {
        int len = 0;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            byte[] bytes = readLine(os);
            len = Integer.valueOf(new String(bytes, StandardCharsets.UTF_8));
        }
        return len;
    }

    private byte[] readLine(ByteArrayOutputStream os) throws IOException {
        boolean foundCR = false;
        while (!foundCR) {
            readBytesIfNotHasRemaining();
            while (bb.hasRemaining()) {
                byte b = bb.get();
                if (b != '\r') {
                    os.write(b);
                } else {
                    foundCR = true;
                    break;
                }
            }
        }
        readBytesIfNotHasRemaining();
        Preconditions.checkState(bb.get() == '\n');

        return os.toByteArray();
    }

    private void readBytesIfNotHasRemaining() throws IOException {
        int read = 0;
        if (!bb.hasRemaining()) {
            bb.clear();

            do {
                read = channel.read(bb);
                if (read > 0) {
                    break;
                }
            } while (true);

            bb.flip();
        }
    }
}