package io.github.parliament.paxos.client;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class InetPeerAcceptors implements PeerAcceptors {
    private static final Logger logger = LoggerFactory.getLogger(InetSocketAddress.class);
    private List<InetSocketAddress> peers;
    private final ConcurrentHashMap<Integer, List<SyncProxyAcceptor>> acceptors = new ConcurrentHashMap<>();
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<InetSocketAddress, ArrayDeque<SocketChannel>> idleChannels
            = new ConcurrentHashMap<>();
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<InetSocketAddress, ArrayDeque<SocketChannel>> busyChannels
            = new ConcurrentHashMap<>();
    private int pmc;

    @Builder
    private InetPeerAcceptors(@NonNull List<InetSocketAddress> peers, int pmc) {
        this.peers = peers;
        if (pmc <= 0) {
            this.pmc = 10;
        } else {
            this.pmc = pmc;
        }
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
                    channel = acquireChannel(address);
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
                releaseChannel(address, channel, a.isIoFailed());
            }
        }
    }

    @Override
    public int done() throws IOException {
        List<SyncProxyAcceptor> others = create0(-1);
        int done = Integer.MAX_VALUE;
        for (SyncProxyAcceptor other : others) {
            int otherDone = other.done();
            done = Math.min(otherDone, done);
        }
        return done;
    }

    @Override
    public int max() throws IOException {
        List<SyncProxyAcceptor> others = create0(-1);
        int done = Integer.MIN_VALUE;
        for (SyncProxyAcceptor other : others) {
            done = Math.max(other.max(), done);
        }
        return done;
    }

    Object lock = new Object();
    SocketChannel acquireChannel(InetSocketAddress address) throws IOException, NoConnectionInPool {
        synchronized (lock) {
            idleChannels.putIfAbsent(address, new ArrayDeque<>());
            busyChannels.putIfAbsent(address, new ArrayDeque<>());

            ArrayDeque<SocketChannel> idle = idleChannels.get(address);
            ArrayDeque<SocketChannel> busy = busyChannels.get(address);

            if (busy.size() >= pmc) {
                throw new NoConnectionInPool(address);
            } else {
                SocketChannel candidate = idle.pollFirst();
                if (candidate != null) {
                    busy.add(candidate);
                    return candidate;
                }
                SocketChannel chn = SocketChannel.open(address);
                busy.add(chn);
                return chn;
            }
        }
    }

    void releaseChannel(InetSocketAddress address, SocketChannel channel, boolean failed) {
        synchronized (lock) {
            if (channel == null) {
                return;
            }
            busyChannels.get(address).remove(channel);
            if (failed) {
                try {
                    channel.close();
                } catch (IOException e) {
                    logger.error("close channel failed.", e);
                }
            } else {
                idleChannels.get(address).add(channel);
            }
        }
    }
}
