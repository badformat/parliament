package io.github.parliament.paxos.server;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespDecoder;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Optional;

public class PaxosProxyReadHandler implements CompletionHandler<Integer, Paxos> {
    private static final Logger logger = LoggerFactory.getLogger(PaxosProxyReadHandler.class);
    @Getter(value = AccessLevel.PACKAGE)
    private AsynchronousSocketChannel channel;
    @Getter(value = AccessLevel.PACKAGE)
    private ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    private RespDecoder respDecoder = new RespDecoder();

    @Builder
    private PaxosProxyReadHandler(@NonNull AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void completed(Integer i, Paxos paxos) {
        ServerCodec codec = new ServerCodec();
        ByteBuffer writeBuffer = null;

        if (i == -1) {
            return;
        }
        byteBuffer.flip();
        respDecoder.decode(this.byteBuffer);
        RespArray array = respDecoder.get();
        if (array == null) {
            return;
        }

        try {
            respDecoder = RespDecoder.create(); // TODO clear in respDecoder.get();
            byteBuffer.clear();
            ServerCodec.Request req = codec.decode(array);
            Acceptor acceptor = paxos.create(req.getRound());
            switch (req.getCmd()) {
                case prepare:
                    Prepare resp = acceptor.prepare(req.getN());
                    writeBuffer = codec.encodePrepare(resp);
                    break;
                case accept:
                    Accept acc = acceptor.accept(req.getN(), req.getV());
                    writeBuffer = codec.encodeAccept(acc);
                    break;
                case decide:
                    acceptor.decide(req.getV());
                    writeBuffer = codec.encodeDecide();
                    break;
                case max:
                    int max = paxos.max();
                    writeBuffer = codec.encodeInt(max);
                    break;
                case min:
                    int min = paxos.min();
                    writeBuffer = codec.encodeInt(min);
                    break;
                case done:
                    int done = paxos.done();
                    writeBuffer = codec.encodeInt(done);
                    break;
                case pull:
                    int rn = req.getRound();
                    byte[] p = paxos.get(rn);
                    writeBuffer = codec.encodeProposal(rn, Optional.ofNullable(p));
                    break;
                default:
                    writeBuffer = codec.encodeError("unknown command :" + req.getCmd());
            }
        } catch (Exception e) {
            writeBuffer = codec.encodeError("proxy error:" + e.getClass().getName() + ".message:" + e.getMessage());
            logger.error("failed in paxos proxy handler.", e);
        } finally {
            Preconditions.checkNotNull(writeBuffer);
            PaxosProxyWriteHandler writeHandler = PaxosProxyWriteHandler.builder().buffer(writeBuffer).channel(channel).build();
            channel.write(writeBuffer, paxos, writeHandler);
        }
    }

    @Override
    public void failed(Throwable exc, Paxos attachment) {
        logger.error("paxos proxy read handler failed", exc);
    }
}
