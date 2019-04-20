package io.github.parliament.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import io.github.parliament.files.DefaultFileService;
import io.github.parliament.rsm.PaxosReplicateStateMachine;
import io.github.parliament.rsm.ProposalPersistenceService;
import lombok.Builder;

/**
 *
 * @author zy
 */
public class KeyValueServer implements KeyValueService {
    private PaxosReplicateStateMachine      machine;
    private InetSocketAddress               kv;
    private AsynchronousServerSocketChannel kvChannel;
    private AsynchronousChannelGroup        channelGroup;

    @Builder
    public KeyValueServer(List<InetSocketAddress> peers, InetSocketAddress me, InetSocketAddress kv, String dir) throws Exception {
        ProposalPersistenceService proposalService = ProposalPersistenceService
                .builder()
                .fileService(new DefaultFileService())
                .path(Paths.get(dir))
                .build();

        machine = PaxosReplicateStateMachine.builder()
                .me(me)
                .peers(peers)
                .threadNo(peers.size() * 2)
                .proposalPersistenceService(proposalService)
                .build();

        this.kv = kv;
    }

    public void start() throws Exception {
        machine.start();

        channelGroup = AsynchronousChannelGroup.withFixedThreadPool(20, Executors.defaultThreadFactory());
        kvChannel = AsynchronousServerSocketChannel.open(channelGroup);
        kvChannel.bind(kv);
        kvChannel.accept(this, new CompletionHandler<AsynchronousSocketChannel, Object>() {
            @Override
            public void completed(AsynchronousSocketChannel channel, Object attachment) {
                kvChannel.accept(attachment, this);
                ByteBuffer bb = ByteBuffer.allocate(1024);
                channel.read(bb, attachment, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(Integer result, Object attachment) {

                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {

                    }
                });
            }

            @Override
            public void failed(Throwable exc, Object attachment) {

            }
        });
    }

    public void shutdown() throws IOException {
        machine.shutdown();
        channelGroup.shutdown();
        kvChannel.close();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        int round = machine.nextRound();

    }

    @Override
    public byte[] putIfAbsent(byte[] key, byte[] value) {
        return new byte[0];
    }

    @Override
    public byte[] compareAndPut(byte[] key, byte[] expect, byte[] update) {
        return new byte[0];
    }

    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }

    @Override
    public boolean del(byte[] key) {
        return false;
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

        KeyValueServer server = KeyValueServer.builder().dir(dir).kv(kv).peers(peers).me(me).build();
        server.start();
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