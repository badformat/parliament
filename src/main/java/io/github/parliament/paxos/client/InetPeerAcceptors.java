package io.github.parliament.paxos.client;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import lombok.Builder;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class InetPeerAcceptors implements PeerAcceptors {
    private static final Logger logger = LoggerFactory.getLogger(InetSocketAddress.class);
    private final ConcurrentHashMap<Integer, List<SyncProxyAcceptor>> acceptors = new ConcurrentHashMap<>();
    private final ConnectionPool connectionPool;
    private final List<InetSocketAddress> peers;

    @Builder
    private InetPeerAcceptors(@NonNull List<InetSocketAddress> peers, @NonNull ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        this.peers = peers;
    }

    @Override
    public List<? extends Acceptor> create(int round) {
        Preconditions.checkState(!acceptors.containsKey(round));
        return create0(round);
    }

    List<SyncProxyAcceptor> create0(int round) {
        return acceptors.computeIfAbsent(round, (r) -> {
            List<SyncProxyAcceptor> a = new ArrayList<>();
            for (InetSocketAddress address : peers) {
                SocketChannel channel = null;
                try {
                    channel = connectionPool.acquireChannel(address);
                    SyncProxyAcceptor proxy = SyncProxyAcceptor.builder()
                            .remote(address)
                            .channel(channel)
                            .round(round)
                            .build();
                    a.add(proxy);
                } catch (IOException | NoConnectionInPool e) {
                    logger.error("create paxos acceptor proxy failed.round {}.address {}.",
                            round, address, e);
                    a.add(new SyncProxyAcceptor(round, address, null) {
                        @Override
                        public Prepare prepare(String n) {
                            return Prepare.reject(n);
                        }

                        @Override
                        public Accept accept(String n, byte[] value) {
                            return Accept.reject(n);
                        }

                        @Override
                        public void decide(byte[] agreement) {

                        }
                    });
                }
            }
            return a;
        });
    }

    @Override
    public void release(int round) {
        List<SyncProxyAcceptor> acc = acceptors.remove(round);
        Preconditions.checkState(acc != null);
        for (SyncProxyAcceptor a : acc) {
            if (a.getChannel() != null) {
                SocketChannel channel = a.getChannel();
                InetSocketAddress address = a.getRemote();
                connectionPool.releaseChannel(address, channel, a.isIoFailed());
            }
        }
    }
}
