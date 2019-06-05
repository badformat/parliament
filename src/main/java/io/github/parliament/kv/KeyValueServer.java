package io.github.parliament.kv;

import io.github.parliament.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author zy
 */
public class KeyValueServer {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueServer.class);

    @Getter(AccessLevel.PACKAGE)
    private InetSocketAddress socketAddress;
    private AsynchronousServerSocketChannel serverSocketChannel;
    private AsynchronousChannelGroup channelGroup;
    private KeyValueEngine engine;

    @Builder
    public KeyValueServer(@NonNull InetSocketAddress socketAddress,
                          @NonNull KeyValueEngine keyValueEngine) {
        this.socketAddress = socketAddress;
        this.engine = keyValueEngine;
    }

    public void start() throws Exception {
        channelGroup = AsynchronousChannelGroup.withFixedThreadPool(20, Executors.defaultThreadFactory());
        serverSocketChannel = AsynchronousServerSocketChannel.open(channelGroup);
        serverSocketChannel.bind(socketAddress);
        serverSocketChannel.accept(this, new CompletionHandler<AsynchronousSocketChannel, Object>() {
            @Override
            public void completed(AsynchronousSocketChannel channel, Object attachment) {
                serverSocketChannel.accept(attachment, this);
                ClientHandler clientHandler = new ClientHandler(channel);
                channel.read(clientHandler.getByteBuffer(), engine, clientHandler);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {

            }
        });

        engine.start();
    }

    public void shutdown() throws IOException {
        channelGroup.shutdown();
        serverSocketChannel.close();
    }

    public static void main(String[] args) throws Exception {
        String prop = System.getProperty("peers");
        List<InetSocketAddress> peers = getPeers(prop);

        prop = System.getProperty("me");
        InetSocketAddress me = getInetSocketAddress(prop);

        prop = System.getProperty("dir");
        String dir = prop;

        prop = System.getProperty("kv");
        InetSocketAddress kv = getInetSocketAddress(prop);

//        ProposalPersistenceService proposalService = ProposalPersistenceService
//                .builder()
//                .fileService(new DefaultFileService())
//                .path(Paths.get(dir))
//                .build();
//
//        PaxosReplicateStateMachine machine = PaxosReplicateStateMachine
//                .builder()
//                .me(me)
//                .peers(peers)
//                .proposalPersistenceService(proposalService)
//                .threadNo(20)
//                .build();
//
//        machine.start();
//        logger.info("本地paxos服务启动成功，地址：" + me);

        @NonNull Persistence pager = PagePersistence.builder().path(Paths.get(dir).resolve("db")).build();
        @NonNull ExecutorService executorService = Executors.newFixedThreadPool(200);
        @NonNull Coordinator coordinator = new Coordinator() {
            @Override
            public void coordinate(int id, byte[] content) {

            }

            @Override
            public Future<byte[]> instance(int id) {
                return null;
            }

            @Override
            public int max() {
                return 0;
            }

            @Override
            public void forget(int before) {

            }
        };
        @NonNull ReplicateStateMachine rsm = ReplicateStateMachine.builder()
                .persistence(PagePersistence.builder().path(Paths.get(dir).resolve("rsm")).build())
                .sequence(new IntegerSequence())
                .coordinator(coordinator)
                .build();
        KeyValueEngine keyValueEngine = KeyValueEngine.builder()
                .persistence(pager)
                .executorService(executorService)
                .rsm(rsm)
                .build();

        KeyValueServer server = KeyValueServer.builder().socketAddress(kv).keyValueEngine(keyValueEngine).build();
        server.start();
        logger.info("本地kv服务启动成功，地址：" + kv);
    }

    static List<InetSocketAddress> getPeers(String prop) {
        String[] peers = prop.split(",");
        List<InetSocketAddress> peerAddrs = new ArrayList<>();
        for (String peer : peers) {
            String[] ipAndPort = peer.split(":");
            peerAddrs.add(new InetSocketAddress(ipAndPort[0], Integer.valueOf(ipAndPort[1])));
        }
        return peerAddrs;
    }

    static InetSocketAddress getInetSocketAddress(String prop) {
        String[] ipAndPort = prop.split(":");
        return new InetSocketAddress(ipAndPort[0], Integer.valueOf(ipAndPort[1]));
    }

}