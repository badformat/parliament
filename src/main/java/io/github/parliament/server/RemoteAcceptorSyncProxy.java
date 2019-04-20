package io.github.parliament.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.RemoteRoundAcceptorProxy;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import lombok.Getter;

public class RemoteAcceptorSyncProxy implements RemoteRoundAcceptorProxy<String> {
    @Getter
    private final InetSocketAddress peer;
    private       SocketChannel     remote;
    private       ClientCodec       codec = new ClientCodec();

    public static class RemoteAcceptor implements Acceptor<String> {
        @Getter
        private final int round;

        private RemoteAcceptorSyncProxy proxy;

        private RemoteAcceptor(int round, RemoteAcceptorSyncProxy proxy) {
            this.round = round;
            this.proxy = proxy;
        }

        @Override
        public Prepare<String> prepare(String n) throws Exception {
            try {
                proxy.initChannelIfNotOpen();
                return proxy.delegatePrepare(round, n);
            } catch (IOException e) {
                proxy.open();
                return proxy.delegatePrepare(round, n);
            }
        }

        @Override
        public Accept<String> accept(String n, byte[] value) throws Exception {
            try {
                proxy.initChannelIfNotOpen();
                return proxy.delegateAccept(round, n, value);
            } catch (IOException e) {
                proxy.open();
                return proxy.delegateAccept(round, n, value);
            }
        }

        @Override
        public void decide(byte[] agreement) throws Exception {
            try {
                proxy.initChannelIfNotOpen();
                proxy.delegateDecide(round, agreement);
            } catch (IOException e) {
                proxy.open();
                proxy.delegateDecide(round, agreement);
            }
        }
    }

    public RemoteAcceptor createAcceptorForRound(int round) {
        return new RemoteAcceptor(round, this);
    }

    public RemoteAcceptorSyncProxy(InetSocketAddress address) {
        this.peer = address;
    }

    @Override
    public Prepare<String> delegatePrepare(int round, String n) throws IOException {
        synchronized (peer) {
            ByteBuffer request = codec.encodePrepare(round, n);
            while (request.hasRemaining()) {
                remote.write(request);
            }

            return codec.decodePrepare(remote, n);
        }
    }

    @Override
    public Accept<String> delegateAccept(int round, String n, byte[] value) throws IOException {
        synchronized (peer) {
            ByteBuffer src = codec.encodeAccept(round, n, value);
            while (src.hasRemaining()) {
                remote.write(src);
            }

            return codec.decodeAccept(remote, n);
        }
    }

    @Override
    public void delegateDecide(int round, byte[] agreement) throws Exception {
        synchronized (peer) {
            Preconditions.checkNotNull(agreement, "decide agreement is null");
            ByteBuffer src = codec.encodeDecide(round, agreement);
            while (src.hasRemaining()) {
                remote.write(src);
            }
            codec.decodeDecide(remote);
        }
    }

    private void open() throws IOException {
        this.remote = SocketChannel.open(peer);
    }

    private void initChannelIfNotOpen() throws IOException {
        if (this.remote == null) {
            this.remote = SocketChannel.open(peer);
        }
    }

}
