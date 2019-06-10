package io.github.parliament.kv;

import io.github.parliament.resp.RespHandlerAttachment;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespReadHandler;
import io.github.parliament.resp.RespWriteHandler;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author zy
 */
public class KeyValueServer {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueServer.class);
    @Getter(AccessLevel.PACKAGE)
    private InetSocketAddress socketAddress;
    private AsynchronousServerSocketChannel serverSocketChannel;
    private AsynchronousChannelGroup channelGroup;
    private KeyValueEngine engine;

    @Builder
    public KeyValueServer(@NonNull InetSocketAddress socketAddress,
                          @NonNull KeyValueEngine keyValueEngine) {
        this.socketAddress = socketAddress;
        this.engine = keyValueEngine;
    }

    public void start() throws Exception {
        engine.start();
        RespReadHandler respReadHandler = new RespReadHandler() {
            @Override
            protected ByteBuffer process(RespHandlerAttachment attachment, RespArray request) throws Exception {
                return engine.submit(request.toBytes())
                        .get(attachment.getTimeOutMills(), TimeUnit.MILLISECONDS)
                        .toByteBuffer();
            }
        };
        RespWriteHandler respWriteHandler = new RespWriteHandler() {
            @Override
            protected void process(RespHandlerAttachment attachment) {

            }
        };
        channelGroup = AsynchronousChannelGroup.withFixedThreadPool(20, Executors.defaultThreadFactory());
        serverSocketChannel = AsynchronousServerSocketChannel.open(channelGroup);
        serverSocketChannel.bind(socketAddress);
        serverSocketChannel.accept(this, new CompletionHandler<AsynchronousSocketChannel, Object>() {
            @Override
            public void completed(AsynchronousSocketChannel channel, Object attachment) {
                if (serverSocketChannel.isOpen()) {
                    serverSocketChannel.accept(attachment, this);
                }

                RespHandlerAttachment respHandlerAttachment = new RespHandlerAttachment(channel, respReadHandler, respWriteHandler);
                channel.read(respHandlerAttachment.getByteBuffer(), respHandlerAttachment, respReadHandler);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("kv server error.", exc);
            }
        });
    }

    public void shutdown() throws IOException {
        channelGroup.shutdown();
        serverSocketChannel.close();
    }
}