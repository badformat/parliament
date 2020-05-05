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
 * 使用java nio监听指定端口，使用{@link io.github.parliament.resp.RespReadHandler}完成redis resp协议解析后，
 * 使用{@link KeyValueEngine}处理，再使用{@link RespWriteHandler}返回响应。
 *
 * @author zy
 */
public class KeyValueServer {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueServer.class);
    // 服务地址
    @Getter(AccessLevel.PACKAGE)
    private InetSocketAddress socketAddress;
    // 服务socket channel
    private AsynchronousServerSocketChannel serverSocketChannel;
    // 处理线程池，问题：accept是否会使用该线程池处理连接请求？不能的原因是什么？
    private AsynchronousChannelGroup channelGroup;
    // kv处理引擎
    private KeyValueEngine engine;

    @Builder
    public KeyValueServer(@NonNull InetSocketAddress socketAddress,
                          @NonNull KeyValueEngine keyValueEngine) {
        this.socketAddress = socketAddress;
        this.engine = keyValueEngine;
    }

    /**
     * 启动服务
     *
     * @throws Exception 异常
     */
    public void start() throws Exception {
        engine.start();
        RespReadHandler respReadHandler = new RespReadHandler() {
            @Override
            protected ByteBuffer process(RespHandlerAttachment attachment, RespArray request) {
                return engine.execute(request.toBytes(), attachment.getTimeOutMills(), TimeUnit.MILLISECONDS);
            }
        };
        RespWriteHandler respWriteHandler = new RespWriteHandler();
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

    /**
     * 关闭服务
     *
     * @throws IOException 关闭异常
     */
    public void shutdown() throws IOException {
        channelGroup.shutdown();
        serverSocketChannel.close();
    }
}