package io.github.parliament.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.RemoteRoundAcceptorProxy;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;
import lombok.Getter;

public class RemoteAcceptorSyncProxy implements RemoteRoundAcceptorProxy<String> {
    @Getter
    private SocketChannel remote;
    private ClientCodec   codec = new ClientCodec();

    public RemoteAcceptorSyncProxy(SocketChannel remote) {
        this.remote = remote;
    }

    @Override
    public Prepare<String> delegatePrepare(int round, String n) throws IOException {
        synchronized (remote) {
            ByteBuffer request = codec.encodePrepare(round, n);
            while (request.hasRemaining()) {
                remote.write(request);
            }

            return codec.decodePrepare(remote, n);
        }
    }

    @Override
    public Accept<String> delegateAccept(int round, String n, byte[] value) throws IOException {
        synchronized (remote) {
            ByteBuffer src = codec.encodeAccept(round, n, value);
            while (src.hasRemaining()) {
                remote.write(src);
            }

            return codec.decodeAccept(remote, n);
        }
    }

    @Override
    public void delegateDecide(int round, byte[] agreement) throws Exception {
        synchronized (remote) {
            Preconditions.checkNotNull(agreement, "decide agreement is null");
            ByteBuffer src = codec.encodeDecide(round, agreement);
            while (src.hasRemaining()) {
                remote.write(src);
            }
            codec.decodeDecide(remote);
        }
    }
}
