package io.github.parliament.paxos.server;

import io.github.parliament.paxos.Paxos;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
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
public class PaxosServer {
    private static final Logger logger = LoggerFactory.getLogger(PaxosServer.class);
    private volatile boolean started = false;
    private volatile InetSocketAddress me;
    @Getter(AccessLevel.PACKAGE)
    private volatile Paxos paxos;
    private AsynchronousChannelGroup channelGroup;
    private AsynchronousServerSocketChannel serverSocketChannel;

    @Builder
    public PaxosServer(InetSocketAddress me, Paxos paxos) {
        this.me = me;
        this.paxos = paxos;
    }

    public void start() throws IOException {
        if (started) {
            throw new IllegalStateException();
        }
        channelGroup = AsynchronousChannelGroup.withFixedThreadPool(20, Executors.defaultThreadFactory());
        serverSocketChannel = AsynchronousServerSocketChannel.open(channelGroup);
        serverSocketChannel.bind(me);

        serverSocketChannel.accept(paxos, new CompletionHandler<>() {

            @Override
            public void completed(AsynchronousSocketChannel channel, Paxos paxos) {
                serverSocketChannel.accept(paxos, this);
                PaxosProxyReadHandler handler = PaxosProxyReadHandler.builder().channel(channel).build();
                channel.read(handler.getByteBuffer(), paxos, handler);
            }

            @Override
            public void failed(Throwable exc, Paxos paxos) {
                logger.error("paxos server socket channel failed.", exc);
            }
        });

        started = true;
    }

    public void shutdown() throws IOException {
        if (!started) {
            return;
        }
        serverSocketChannel.close();
        channelGroup.shutdown();
        started = false;
    }
}