package io.github.parliament.paxos.client;

import lombok.AccessLevel;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<InetSocketAddress, ArrayDeque<SocketChannel>> idles
            = new ConcurrentHashMap<>();
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<InetSocketAddress, ArrayDeque<SocketChannel>> actives
            = new ConcurrentHashMap<>();
    @Getter
    private int poolSize;

    public synchronized static ConnectionPool create(int poolSize) {
        return new ConnectionPool(poolSize);
    }

    private ConnectionPool(int poolSize) {
        if (poolSize <= 0) {
            this.poolSize = 10;
        } else {
            this.poolSize = poolSize;
        }
    }

    synchronized SocketChannel acquireChannel(InetSocketAddress address) throws IOException, NoConnectionInPool {
        idles.putIfAbsent(address, new ArrayDeque<>());
        actives.putIfAbsent(address, new ArrayDeque<>());

        ArrayDeque<SocketChannel> idle = idles.get(address);
        ArrayDeque<SocketChannel> busy = actives.get(address);

        if (busy.size() >= poolSize) {
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

    synchronized void releaseChannel(InetSocketAddress address, SocketChannel channel, boolean failed) {
        if (channel == null) {
            return;
        }
        actives.get(address).remove(channel);
        if (failed) {
            try {
                channel.close();
            } catch (IOException e) {
                logger.error("close channel failed.", e);
            }
        } else {
            idles.get(address).add(channel);
        }
    }

}
