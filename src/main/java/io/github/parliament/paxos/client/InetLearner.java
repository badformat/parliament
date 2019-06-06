package io.github.parliament.paxos.client;

import io.github.parliament.Coordinator;
import lombok.Builder;
import lombok.Builder.Default;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

@Builder
public class InetLearner {
    private static final Logger logger = LoggerFactory.getLogger(InetLearner.class);
    private volatile List<InetSocketAddress> others;
    @Default
    private ClientCodec codec = new ClientCodec();
    private Coordinator coordinator;

    public boolean syncFrom(int begin) {
        int max = 0;
        try {
            max = learnMax().stream().reduce(-1, (a, b) -> b > a ? b : a);
            coordinator.max(max);
            if (begin < max) {
                sync(begin, max);
            }
            return true;
        } catch (Exception e) {
            logger.error("syncFrom error:", e);
            return false;
        }
    }

    public boolean sync(int round) {
        try {
            sync(round, round);
            return true;
        } catch (Exception e) {
            logger.error("sync error:", e);
            return false;
        }
    }

    private void sync(int begin, int end) throws Exception {
        BitSet bs = new BitSet(end - begin + 1);
        for (InetSocketAddress peer : others) {
            int round = begin;
            try (SocketChannel remote = SocketChannel.open(peer)) {
                while (round <= end) {
                    int i = round - begin;
                    if (!bs.get(i) && sync(remote, round)) {
                        bs.set(i);
                        round++;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    private boolean sync(SocketChannel remote, int round) {
        try {
            ByteBuffer cmd = codec.encodePull(round);
            while (cmd.hasRemaining()) {
                remote.write(cmd);
            }
            Optional<byte[]> ret = codec.decodePull(round, remote);
            if (!ret.isPresent()) {
                return false;
            } else {
                coordinator.instance(round, ret.get());
            }
            return true;
        } catch (Exception e) {
            logger.error("failed in sync. round: {}", round, e);
            return false;
        }
    }

    public List<Integer> learnMin() throws IOException {
        List<Integer> mins = new ArrayList<>();
        for (InetSocketAddress peer : others) {
            try (SocketChannel remote = SocketChannel.open(peer)) {
                ByteBuffer cmd = codec.encodeMin();
                while (cmd.hasRemaining()) {
                    remote.write(cmd);
                }
                mins.add(codec.decodeMin(remote));
            }
        }
        return mins;
    }

    public List<Integer> learnMax() throws IOException {
        List<Integer> maxs = new ArrayList<>();
        for (InetSocketAddress peer : others) {
            try (SocketChannel remote = SocketChannel.open(peer)) {
                ByteBuffer cmd = codec.encodeMax();
                while (cmd.hasRemaining()) {
                    remote.write(cmd);
                }
                maxs.add(codec.decodeMax(remote));
            }
        }
        return maxs;
    }
}