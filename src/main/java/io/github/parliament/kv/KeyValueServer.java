package io.github.parliament.kv;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Executors;

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
        channelGroup = AsynchronousChannelGroup.withFixedThreadPool(20, Executors.defaultThreadFactory());
        serverSocketChannel = AsynchronousServerSocketChannel.open(channelGroup);
        serverSocketChannel.bind(socketAddress);
        serverSocketChannel.accept(this, new CompletionHandler<AsynchronousSocketChannel, Object>() {
            @Override
            public void completed(AsynchronousSocketChannel channel, Object attachment) {
                serverSocketChannel.accept(attachment, this);
                ReadHandler readHandler = new ReadHandler(channel);
                channel.read(readHandler.getByteBuffer(), engine, readHandler);
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