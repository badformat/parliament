package io.github.parliament.resp.reader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;

import com.google.common.base.Preconditions;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespSimpleString;

public class RespStreamReader {
    private SocketChannel channel;
    ByteBuffer bb;

    public static RespStreamReader with(SocketChannel socketChannel) {
        return new RespStreamReader(socketChannel, 1024);
    }

    private RespStreamReader(SocketChannel socketChannel, int i) {
        this.channel = socketChannel;
        bb = ByteBuffer.allocate(64);
        bb.flip();
    }

    public synchronized Optional<RespSimpleString> getSimpleString(Charset charset)
            throws IOException {
        byte firstByte = getRespFirstByte();
        if (firstByte != RespSimpleString.firstByte) {
            return Optional.empty();
        }

        int n = 0;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream(512)) {
            boolean finished = false;
            do {
                while (bb.hasRemaining()) {
                    byte c = bb.get();
                    os.write(c);
                    if (c == '\n') {
                        finished = true;
                    }
                }
                if (finished) {
                    break;
                }
                bb.clear();
                n = channel.read(bb);
            } while (n != -1);

            byte[] bytes = os.toByteArray();
            Preconditions.checkState(bytes.length >= 2);
            Preconditions.checkState(bytes[bytes.length - 2] == '\r');
            bytes = Arrays.copyOf(bytes, bytes.length - 2);
            return Optional.of(RespSimpleString.with(new String(bytes, charset), charset));
        }
    }

    public synchronized Optional<RespBulkString> getBulkString() throws IOException {
        byte firstByte = getRespFirstByte();
        if (firstByte != RespBulkString.firstByte) {
            return Optional.empty();
        }
        return null;
    }

    //private Optional<String> readLine() {
    //    do {
    //        while (bb.hasRemaining()) {
    //            byte c = bb.get();
    //            os.write(c);
    //            if (c == '\n') {
    //                finished = true;
    //            }
    //        }
    //        if (finished) {
    //            break;
    //        }
    //        bb.clear();
    //        n = channel.read(bb);
    //    } while (n != -1);
    //}

    private byte getRespFirstByte() throws IOException {
        int n = 0;

        if (!bb.hasRemaining()) {
            bb.clear();
            do {
                n = channel.read(bb);
            } while (n == 0);

            if (n == -1) {
                throw new IllegalStateException("");
            }

            bb.flip();
        }

        return bb.get();
    }
}
