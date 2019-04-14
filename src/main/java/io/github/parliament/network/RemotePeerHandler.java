package io.github.parliament.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.network.AcceptorRequestDeserializer.AcceptorRequest;
import lombok.Getter;

/**
 *
 * @author zy
 */
public class RemotePeerHandler extends Thread {
    @Getter
    private          SocketChannel clientChannel;
    @Getter
    private volatile boolean       dead = false;

    private volatile AcceptorFactory<String> acceptorFactory;

    private AcceptorRequestDeserializer deserializer = new AcceptorRequestDeserializer();
    private AcceptorResponseSerializer  serializer   = new AcceptorResponseSerializer();

    RemotePeerHandler(SocketChannel clientChannel, AcceptorFactory<String> acceptorFactory) {
        this.clientChannel = clientChannel;
        this.acceptorFactory = acceptorFactory;
    }

    @Override
    public void run() {
        do {
            try {
                AcceptorRequest req = deserializer.deserializeRequest(clientChannel);
                ByteBuffer src;
                Acceptor<String> acceptor = acceptorFactory.createLocalAcceptorFor(
                        req.getRound());
                switch (req.getCmd()) {
                    case prepare:
                        Prepare<String> resp = acceptor.prepare(req.getN());
                        src = serializer.serializerPrepareResponse(resp);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case accept:
                        Accept<String> accresp = acceptor.accept(req.getN(), req.getV());
                        src = serializer.serializerAcceptResponse(accresp);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case decide:
                        acceptor.decide(req.getV());
                        src = serializer.serializerDecideResponse();
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    default:
                        throw new IllegalStateException();
                }
            } catch (IOException e) {
                //TODO
                e.printStackTrace();
                dead = true;
                return;
            } catch (Exception e) {
                dead = true;
                e.printStackTrace();
                try {
                    ByteBuffer src = serializer.serializeErrorResponse("Exception：" + e.getMessage());
                    while (src.hasRemaining()) {
                        clientChannel.write(src);
                    }
                    clientChannel.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                    return;
                }
            }
        } while (!dead);
    }
}
