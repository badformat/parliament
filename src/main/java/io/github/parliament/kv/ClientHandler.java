package io.github.parliament.kv;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespDecoder;
import io.github.parliament.resp.RespError;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

/**
 *
 * @author zy
 */
class ClientHandler implements CompletionHandler<Integer, KeyValueEngine> {
    private AsynchronousSocketChannel channel;
    @Getter(value = AccessLevel.PACKAGE)
    private ByteBuffer                byteBuffer  = ByteBuffer.allocate(2048);
    private RespDecoder               respDecoder = new RespDecoder();
    private int                       timeOutMs   = 0;

    ClientHandler(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    @Builder
    ClientHandler(AsynchronousSocketChannel channel, int timeoutMs) {
        this.channel = channel;
        this.timeOutMs = timeoutMs;
    }

    @Override
    public void completed(Integer result, KeyValueEngine engine) {
        if (result == -1) {
            return;
        }

        try {
            byteBuffer.flip();
            respDecoder.decode(byteBuffer);

            RespArray cmd = respDecoder.get();
            if (cmd != null) {
                ByteBuffer bb = engine.execute(cmd).get(timeOutMs, TimeUnit.MILLISECONDS).toByteBuffer();
                while (bb.hasRemaining()) {
                    channel.write(bb);
                }
            }
        } catch (Exception e) {
            ByteBuffer error = RespError.withUTF8("Error:" + e.getMessage()).toByteBuffer();
            while (error.hasRemaining()) {
                channel.write(error);
            }
        } finally {
            byteBuffer.clear();
            channel.read(byteBuffer, engine, this);
        }
    }

    @Override
    public void failed(Throwable exc, KeyValueEngine attachment) {

    }
}