package io.github.parliament.paxos.client;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class SyncProxyAcceptor implements Acceptor {
    @Getter(AccessLevel.PACKAGE)
    private InetSocketAddress remote;
    @Getter(AccessLevel.PACKAGE)
    private SocketChannel channel;
    private ClientCodec codec = new ClientCodec();
    private int round;
    @Getter(AccessLevel.PACKAGE)
    private volatile boolean ioFailed = false;

    @Builder
    protected SyncProxyAcceptor(int round, InetSocketAddress remote, SocketChannel channel) {
        this.round = round;
        this.remote = remote;
        this.channel = channel;
    }

    @Override
    public Prepare prepare(String n) throws IOException {
        try {
            return delegatePrepare(round, n);
        } catch (IOException e) {
            ioFailed = true;
            throw e;
        }
    }

    @Override
    public Accept accept(String n, byte[] value) throws Exception {
        try {
            return delegateAccept(round, n, value);
        } catch (IOException e) {
            ioFailed = true;
            throw e;
        }
    }

    @Override
    public void decide(byte[] agreement) throws Exception {
        try {
            delegateDecide(round, agreement);
        } catch (IOException e) {
            ioFailed = true;
            throw e;
        }
    }

    Prepare delegatePrepare(int round, String n) throws IOException {
        synchronized (channel) {
            ByteBuffer request = codec.encodePrepare(round, n);
            while (request.hasRemaining()) {
                channel.write(request);
            }

            return codec.decodePrepare(channel, n);
        }
    }

    Accept delegateAccept(int round, String n, byte[] value) throws IOException {
        synchronized (channel) {
            ByteBuffer src = codec.encodeAccept(round, n, value);
            while (src.hasRemaining()) {
                channel.write(src);
            }

            return codec.decodeAccept(channel, n);
        }
    }

    void delegateDecide(int round, byte[] agreement) throws Exception {
        synchronized (channel) {
            Preconditions.checkNotNull(agreement, "decide agreement is null");
            ByteBuffer src = codec.encodeDecide(round, agreement);
            while (src.hasRemaining()) {
                channel.write(src);
            }
            codec.decodeDecide(channel);
        }
    }
}
