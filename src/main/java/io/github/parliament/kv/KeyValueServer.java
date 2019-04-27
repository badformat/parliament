package io.github.parliament.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author zy
 */
public class KeyValueServer {
    @Getter(AccessLevel.PACKAGE)
    private InetSocketAddress               socketAddress;
    private AsynchronousServerSocketChannel serverSocketChannel;
    private AsynchronousChannelGroup        channelGroup;
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private KeyValueEngine                  keyValueEngine;

    @Builder
    public KeyValueServer(InetSocketAddress socketAddress) throws Exception {
        this.socketAddress = socketAddress;
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

        KeyValueServer server = KeyValueServer.builder().socketAddress(kv).build();
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