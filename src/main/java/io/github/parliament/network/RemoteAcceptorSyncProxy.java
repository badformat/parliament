package io.github.parliament.network;

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
    private SocketChannel                remote;
    private AcceptorRequestSerializer    serializer   = new AcceptorRequestSerializer();
    private AcceptorResponseDeserializer deserializer = new AcceptorResponseDeserializer();

    public RemoteAcceptorSyncProxy(SocketChannel remote) {
        this.remote = remote;
    }

    @Override
    public Prepare<String> delegatePrepare(int round, String n) throws IOException {
        synchronized (remote) {
            ByteBuffer request = serializer.serializePrepare(round, n);
            while (request.hasRemaining()) {
                remote.write(request);
            }

            return deserializer.deserializePrepare(remote, n);
        }
    }

    @Override
    public Accept<String> delegateAccept(int round, String n, byte[] value) throws IOException {
        synchronized (remote) {
            ByteBuffer src = serializer.serializeAccept(round, n, value);
            while (src.hasRemaining()) {
                remote.write(src);
            }

            return deserializer.deserializeAccept(remote, n);
        }
    }

    @Override
    public void delegateDecide(int round, byte[] agreement) throws Exception {
        synchronized (remote) {
            Preconditions.checkNotNull(agreement, "decide agreement is null");
            ByteBuffer src = serializer.serializeDecide(round, agreement);
            while (src.hasRemaining()) {
                remote.write(src);
            }
            deserializer.deserializeDecide(remote);
        }
    }
}
