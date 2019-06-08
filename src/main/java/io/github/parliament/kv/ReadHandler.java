package io.github.parliament.kv;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespDecoder;
import io.github.parliament.resp.RespError;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author zy
 */
class ReadHandler implements CompletionHandler<Integer, KeyValueEngine> {
    private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);
    @Getter(value = AccessLevel.PACKAGE)
    private AsynchronousSocketChannel channel;
    @Getter(value = AccessLevel.PACKAGE)
    private ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    private RespDecoder respDecoder = new RespDecoder();
    private int timeOutMs = 2000;

    ReadHandler(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    @Builder
    ReadHandler(AsynchronousSocketChannel channel, int timeoutMs) {
        this.channel = channel;
        this.timeOutMs = timeoutMs;
    }

    @Override
    public void completed(Integer result, KeyValueEngine engine) {
        if (result == -1) {
            return;
        }
        ByteBuffer bb;
        try {
            byteBuffer.flip();
            respDecoder.decode(byteBuffer);

            RespArray cmd = respDecoder.get();
            if (cmd != null) {
                bb = engine.execute(cmd.toBytes()).get(timeOutMs, TimeUnit.MILLISECONDS).toByteBuffer();
                channel.write(bb, engine, new WriteHandler(this));
            }
        } catch (TimeoutException | IOException | InterruptedException | UnknownKeyValueCommand |
                ExecutionException e) {
            logger.error("kv engine execute failed.", e);
            bb = RespError.withUTF8("kv engine failed.Exception:" + e.getClass().getName()).toByteBuffer();
            channel.write(bb, engine, new WriteHandler(this));
        } finally {
            byteBuffer.clear();
        }
    }

    @Override
    public void failed(Throwable exc, KeyValueEngine attachment) {
        logger.error("kv server channel failed.", exc);
    }
}