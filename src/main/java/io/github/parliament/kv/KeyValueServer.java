package io.github.parliament.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import io.github.parliament.KeyValueEngine;
import io.github.parliament.files.DefaultFileService;
import io.github.parliament.rsm.PaxosReplicateStateMachine;
import io.github.parliament.rsm.ProposalPersistenceService;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author zy
 */
public class KeyValueServer {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueServer.class);

    @Getter(AccessLevel.PACKAGE)
    private InetSocketAddress               socketAddress;
    private AsynchronousServerSocketChannel serverSocketChannel;
    private AsynchronousChannelGroup        channelGroup;
    private KeyValueEngine keyValueEngine;

    @Builder
    public KeyValueServer(@NonNull InetSocketAddress socketAddress, @NonNull KeyValueEngine keyValueEngine) throws Exception {
        this.socketAddress = socketAddress;
        this.keyValueEngine = keyValueEngine;
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
                channel.read(clientHandler.getByteBuffer(), keyValueEngine, clientHandler);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {

            }
        });

        keyValueEngine.start();
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

        ProposalPersistenceService proposalService = ProposalPersistenceService
                .builder()
                .fileService(new DefaultFileService())
                .path(Paths.get(dir))
                .build();

        PaxosReplicateStateMachine machine = PaxosReplicateStateMachine
                .builder()
                .me(me)
                .peers(peers)
                .proposalPersistenceService(proposalService)
                .threadNo(20)
                .build();

        machine.start();
        logger.info("本地paxos服务启动成功，地址：" + me);

        KeyValueEngine keyValueEngine = PaxosKeyValueEngine.builder().path(Paths.get(dir).resolve("db")).rsm(machine).build();

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