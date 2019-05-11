package io.github.parliament.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Optional;

import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.server.ServerCodec.Request;
import lombok.Getter;

/**
 *
 * @author zy
 */
public class RemotePeerHandler extends Thread {
    @Getter
    private SocketChannel clientChannel;

    private volatile AcceptorFactory<String> acceptorFactory;
    private          ProposalService         proposalService;

    private ServerCodec codec = new ServerCodec();

    RemotePeerHandler(SocketChannel clientChannel, AcceptorFactory<String> acceptorFactory, ProposalService proposalService) {
        this.clientChannel = clientChannel;
        this.acceptorFactory = acceptorFactory;
        this.proposalService = proposalService;
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
                Acceptor<String> acceptor = acceptorFactory.createLocalAcceptorFor(
                        req.getRound());
                switch (req.getCmd()) {
                    case prepare:
                        Prepare<String> resp = acceptor.prepare(req.getN());
                        src = codec.encodePrepare(resp);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case accept:
                        Accept<String> acc = acceptor.accept(req.getN(), req.getV());
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
                        int max = proposalService.maxRound();
                        src = codec.encodeInt(max);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case min:
                        int min = proposalService.minRound();
                        src = codec.encodeInt(min);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    case pull:
                        int rn = req.getRound();
                        Optional<byte[]> p = proposalService.getProposal(rn);
                        src = codec.encodeProposal(rn, p);
                        while (src.hasRemaining()) {
                            clientChannel.write(src);
                        }
                        break;
                    default:
                        throw new IllegalStateException();
                }

            } while (true);
        } catch (Exception e) {
            //e.printStackTrace();
        }

        if (clientChannel.isConnected()) {
            try {
                clientChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
