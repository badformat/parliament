package org.parliament.resp.reader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.parliament.resp.RespArray;
import org.parliament.resp.RespBulkString;
import org.parliament.resp.RespData;
import org.parliament.resp.RespError;
import org.parliament.resp.RespInteger;
import org.parliament.resp.RespSimpleString;

import com.google.common.base.Preconditions;

public class RespSeekableReader {
    private SeekableByteChannel byteChannel;
    private int postion = 0;
    private ByteBuffer bb;

    public static RespSeekableReader with(SeekableByteChannel byteChannel) {
        return new RespSeekableReader(byteChannel, 1024);
    }

    public static RespSeekableReader with(SeekableByteChannel byteChannel, int bufferCapacity) {
        return new RespSeekableReader(byteChannel, bufferCapacity);
    }

    private RespSeekableReader(SeekableByteChannel byteChannel, int bufferCapacity) {
        this.byteChannel = byteChannel;
        this.bb = ByteBuffer.allocate(bufferCapacity);
    }

    public synchronized Optional<RespSimpleString> getSimpleString(Charset charset) throws IOException {
        return consumeString(charset, '+').map(c -> {
            return new RespSimpleString(c, charset);
        });
    }

    public synchronized Optional<RespError> getError(Charset charset) throws IOException {
        return consumeString(charset, '-').map(c -> {
            return new RespError(c, charset);
        });
    }

    public synchronized Optional<RespInteger> getInteger(Charset charset) throws IOException {
        return consumeString(charset, ':').map(c -> {
            return new RespInteger(c, charset);
        });
    }

    public synchronized Optional<RespBulkString> getBulkString() throws IOException {
        Optional<String> content = consumeString(StandardCharsets.UTF_8, '$');
        if (!content.isPresent()) {
            return Optional.empty();
        }

        int len = Integer.valueOf(content.get());
        if (len == -1) {
            return Optional.of(new RespBulkString(null));
        }
        if (len < -1) {
            throw new IllegalStateException("");
        }
        byte[] bytes = consumeBytesEndWithCRLF(len);

        return Optional.of(new RespBulkString(bytes));
    }

    public synchronized Optional<RespArray> getArray(Charset charset) throws IOException {
        Optional<String> content = consumeString(charset, '*');
        if (!content.isPresent()) {
            return Optional.empty();
        }

        int len = Integer.valueOf(content.get());
        Preconditions.checkState(len >= 0, "");
        if (len == 0) {
            return Optional.of(RespArray.empty());
        }
        List<RespData> datas = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            byte b = getNextByte();
            switch (b) {
            case '+':
                datas.add(this.getSimpleString(charset).get());
                break;
            case '-':
                datas.add(this.getError(charset).get());
                break;
            case ':':
                datas.add(this.getInteger(charset).get());
                break;
            case '$':
                datas.add(this.getBulkString().get());
                break;
            case '*':
                datas.add(this.getArray(charset).get());
                break;
            }
        }

        return Optional.of(RespArray.with(datas));
    }

    byte getNextByte() throws IOException {
        int n = 0;
        bb.clear();
        for (;;) {
            do {
                n = byteChannel.read(bb);
            } while (n == 0);

            if (bb.position() == 0) {
                throw new IllegalStateException("");
            }

            bb.flip();
            byteChannel.position(postion);
            return bb.get();
        }
    }

    void consumeCRLF() throws IOException {
        consumeBytesEndWithCRLF(0);
    }

    byte[] consumeBytesEndWithCRLF(int len) throws IOException {
        byteChannel.position(postion);
        int n = 0;
        int contentRemain = len + 2;
        bb.clear();
        try (ByteArrayOutputStream os = new ByteArrayOutputStream(512)) {
            for (;;) {
                do {
                    n = byteChannel.read(bb);
                } while (n == 0);

                if (bb.position() == 0) {
                    throw new IllegalStateException("");
                }

                bb.flip();
                while (bb.hasRemaining() && contentRemain > 0) {
                    postion++;
                    contentRemain--;
                    os.write(bb.get());
                }

                if (contentRemain == 0) {
                    break;
                }
                bb.clear();
            }
            byte[] bytes = os.toByteArray();
            Preconditions.checkState(bytes[bytes.length - 1] == '\n');
            Preconditions.checkState(bytes[bytes.length - 2] == '\r');
            return Arrays.copyOf(bytes, bytes.length - 2);
        }
    }

    Optional<String> consumeString(Charset charset, char firstByte) throws IOException {
        byteChannel.position(postion);
        int n = 0;
        bb.clear();
        try (ByteArrayOutputStream os = new ByteArrayOutputStream(512)) {
            do {
                n = byteChannel.read(bb);
            } while (n == 0);

            if (bb.position() == 0) {
                return Optional.empty();
            }

            bb.flip();
            if (bb.get() != firstByte) {
                return Optional.empty();
            }

            postion++;
            boolean foundCR = false;
            for (;;) {
                while (bb.hasRemaining()) {
                    byte b = bb.get();
                    postion++;
                    if (foundCR && b == '\n') {
                        String content = new String(os.toByteArray(), charset);
                        return Optional.of(content);
                    } else if (foundCR) {
                        throw new IllegalStateException("");
                    }
                    if (b != '\r') {
                        os.write(b);
                    } else {
                        foundCR = true;
                    }
                }

                bb.clear();
                do {
                    n = byteChannel.read(bb);
                } while (n == 0);
                if (bb.position() == 0) {
                    throw new IllegalStateException("");
                }
                bb.flip();
            }
        }
    }
}
