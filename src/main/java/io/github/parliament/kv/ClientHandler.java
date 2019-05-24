package io.github.parliament.kv;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespDecoder;
import io.github.parliament.resp.RespError;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author zy
 */
class ClientHandler implements CompletionHandler<Integer, KeyValueEngine> {
    private static final Logger                    logger      = LoggerFactory.getLogger(ClientHandler.class);
    private              AsynchronousSocketChannel channel;
    @Getter(value = AccessLevel.PACKAGE)
    private              ByteBuffer                byteBuffer  = ByteBuffer.allocate(2048);
    private              RespDecoder               respDecoder = new RespDecoder();
    private              int                       timeOutMs   = 2000;

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
        } catch (TimeoutException e) {
            logger.error("处理请求异常", e);
            ByteBuffer error = RespError.withUTF8("server process timeout").toByteBuffer();
            while (error.hasRemaining()) {
                channel.write(error);
            }
        } catch (Exception e) {
            logger.error("处理请求异常", e);
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