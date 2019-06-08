package io.github.parliament;

import io.github.parliament.kv.KeyValueEngine;
import io.github.parliament.kv.KeyValueServer;
import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.TimestampSequence;
import io.github.parliament.paxos.client.InetPeerAcceptors;
import io.github.parliament.paxos.client.PeerAcceptors;
import io.github.parliament.paxos.server.PaxosServer;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        String prop = System.getProperty("PeerAcceptors");
        List<InetSocketAddress> peers = getPeers(prop);

        prop = System.getProperty("me");
        InetSocketAddress me = getInetSocketAddress(prop);

        prop = System.getProperty("dir");
        String dir = prop;

        prop = System.getProperty("kv");
        InetSocketAddress kv = getInetSocketAddress(prop);


        @NonNull ExecutorService executorService = Executors.newFixedThreadPool(200);
        @NonNull PeerAcceptors acceptors = InetPeerAcceptors.builder().peers(peers).pmc(20).build();
        @NonNull Paxos paxos = Paxos.builder()
                .executorService(executorService)
                .peerAcceptors(acceptors)
                .persistence(PagePersistence.builder().path(Paths.get(dir).resolve("paxos")).build())
                .sequence(new TimestampSequence())
                .build();

        PaxosServer paxosServer = PaxosServer.builder()
                .paxos(paxos)
                .me(me)
                .build();

        paxosServer.start();
        logger.info("本地paxos服务启动成功，地址：" + me);

        @NonNull ReplicateStateMachine rsm = ReplicateStateMachine.builder()
                .persistence(PagePersistence.builder().path(Paths.get(dir).resolve("rsm")).build())
                .sequence(new IntegerSequence())
                .coordinator(paxos)
                .build();

        KeyValueEngine keyValueEngine = KeyValueEngine.builder()
                .persistence(PagePersistence.builder().path(Paths.get(dir).resolve("db")).build())
                .executorService(executorService)
                .rsm(rsm)
                .build();

        KeyValueServer server = KeyValueServer.builder().socketAddress(kv).keyValueEngine(keyValueEngine).build();
        server.start();
        logger.info("本地kv服务启动成功，地址：" + kv);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("服务进程退出.");
            try {
                server.shutdown();
                paxosServer.shutdown();
            } catch (IOException e) {
            }
        }));
    }

    public static List<InetSocketAddress> getPeers(String prop) {
        String[] peers = prop.split(",");
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String peer : peers) {
            String[] ipAndPort = peer.split(":");
            addresses.add(new InetSocketAddress(ipAndPort[0], Integer.valueOf(ipAndPort[1])));
        }
        return addresses;
    }

    public static InetSocketAddress getInetSocketAddress(String prop) {
        String[] ipAndPort = prop.split(":");
        return new InetSocketAddress(ipAndPort[0], Integer.valueOf(ipAndPort[1]));
    }
}
