package io.github.parliament.paxos.server;

import io.github.parliament.Coordinator;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.LocalAcceptors;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.paxos.server.ServerCodec.Request;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Optional;

/**
 * @author zy
 */
public class RemotePeerHandler extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(RemotePeerHandler.class);
    @Getter
    private SocketChannel clientChannel;

    private volatile LocalAcceptors localAcceptors;
    private Coordinator coordinator;

    private ServerCodec codec = new ServerCodec();

    RemotePeerHandler(SocketChannel clientChannel, LocalAcceptors localAcceptors, Coordinator coordinator) {
        this.clientChannel = clientChannel;
        this.localAcceptors = localAcceptors;
        this.coordinator = coordinator;
    }

    @Override
    public void run() {
        try {
            do {
                if (!clientChannel.isConnected()) {
                    return;
                }
                Request req = codec.decode(clientChannel);
                ByteBuffer src;
                Acceptor acceptor = localAcceptors.create(
                        req.getRound());
                switch (req.getCmd()) {
                    case prepare:
                        Prepare resp = acceptor.prepare(req.getN());
                        src = codec.encodePrepare(resp);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case accept:
                        Accept acc = acceptor.accept(req.getN(), req.getV());
                        src = codec.encodeAccept(acc);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case decide:
                        acceptor.decide(req.getV());
                        src = codec.encodeDecide();
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case max:
                        int max = coordinator.max();
                        src = codec.encodeInt(max);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case min:
                        int min = coordinator.min();
                        src = codec.encodeInt(min);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case pull:
                        int rn = req.getRound();
                        byte[] p = coordinator.get(rn);
                        src = codec.encodeProposal(rn, Optional.ofNullable(p));
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    default:
                        throw new IllegalStateException();
                }

            } while (true);
        } catch (Exception e) {
            logger.error("fail in remote peer.", e);
        }

        if (clientChannel.isConnected()) {
            try {
                clientChannel.close();
            } catch (IOException e) {
                logger.error("fail in remote peer close.", e);
            }
        }
    }
}
