package io.github.parliament.paxos.server;

import io.github.parliament.paxos.Paxos;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

public class PaxosProxyWriteHandler implements CompletionHandler<Integer, Paxos> {
    private static final Logger logger = LoggerFactory.getLogger(PaxosProxyWriteHandler.class);
    @Getter(value = AccessLevel.PACKAGE)
    private AsynchronousSocketChannel channel;
    @Getter(value = AccessLevel.PACKAGE)
    private ByteBuffer byteBuffer;

    @Builder
    private PaxosProxyWriteHandler(@NonNull AsynchronousSocketChannel channel, ByteBuffer buffer) {
        this.channel = channel;
        this.byteBuffer = buffer;
    }

    @Override
    public void completed(Integer i, Paxos paxos) {
        if (i == -1) {
            return;
        }

        if (byteBuffer.hasRemaining()) {
            channel.write(byteBuffer, paxos, this);
            return;
        }

        byteBuffer.clear();
        PaxosProxyReadHandler readHandler = PaxosProxyReadHandler.builder().channel(channel).build();
        channel.read(readHandler.getByteBuffer(), paxos, readHandler);
    }

    @Override
    public void failed(Throwable exc, Paxos paxos) {
        logger.error("paxos proxy write handler failed", exc);
    }
}
