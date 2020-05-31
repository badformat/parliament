package io.github.parliament.paxos.server;

import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespHandlerAttachment;
import io.github.parliament.resp.RespReadHandler;
import io.github.parliament.resp.RespWriteHandler;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Optional;
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

        PaxosRespReadHandler readHandler = new PaxosRespReadHandler();
        RespWriteHandler writeHandler = new RespWriteHandler();

        serverSocketChannel.accept(paxos, new CompletionHandler<AsynchronousSocketChannel, Paxos>() {

            @Override
            public void completed(AsynchronousSocketChannel channel, Paxos paxos) {
                if (serverSocketChannel.isOpen()) {
                    serverSocketChannel.accept(paxos, this);
                }
                RespHandlerAttachment attachment = new RespHandlerAttachment(channel, readHandler, writeHandler);
                channel.read(attachment.getByteBuffer(), attachment, readHandler);
            }

            @Override
            public void failed(Throwable exc, Paxos paxos) {
                logger.error("Paxos server channel发生错误.", exc);
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

    class PaxosRespReadHandler extends RespReadHandler {
        @Override
        protected ByteBuffer process(RespHandlerAttachment attachment, RespArray array) throws Exception {
            ServerCodec codec = new ServerCodec();
            ServerCodec.Request req = codec.decode(array);
            Acceptor acceptor = paxos.create(req.getRound());

            switch (req.getCmd()) {
                case prepare:
                    Prepare resp = acceptor.prepare(req.getN());
                    return codec.encodePrepare(resp);
                case accept:
                    Accept acc = acceptor.accept(req.getN(), req.getV());
                    return codec.encodeAccept(acc);
                case decide:
                    acceptor.decide(req.getV());
                    return codec.encodeDecide();
                case max:
                    int max = paxos.max();
                    return codec.encodeInt(max);
                case min:
                    int min = paxos.min();
                    return codec.encodeInt(min);
                case done:
                    int done = paxos.done();
                    return codec.encodeInt(done);
                case pull:
                    int rn = req.getRound();
                    byte[] p = paxos.get(rn);
                    return codec.encodeProposal(rn, Optional.ofNullable(p));
                default:
                    return codec.encodeError("未知的Paxos服务命令：" + req.getCmd());
            }
        }
    }
}