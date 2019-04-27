package io.github.parliament.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import io.github.parliament.paxos.Proposal;
import lombok.Builder;
import lombok.Builder.Default;

@Builder
public class InetLearner {
    private volatile List<InetSocketAddress> others;
    @Default
    private          ClientCodec             codec = new ClientCodec();
    private          ProposalService         proposalService;

    public boolean pullAll(int begin) {
        int max = 0;
        try {
            max = learnMax().stream().reduce(0, (a, b) -> b > a ? b : a);
            proposalService.updateMaxRound(max);
            if (begin < max) {
                pull(begin, max);
            }
            return true;
        } catch (Exception e) {
            //TODO
            e.printStackTrace();
            return false;
        }
    }

    public boolean pull(int round) {
        try {
            pull(round, round);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private void pull(int begin, int end) throws Exception {
        BitSet bs = new BitSet(end - begin + 1);
        for (InetSocketAddress peer : others) {
            int round = begin;
            try (SocketChannel remote = SocketChannel.open(peer)) {
                while (round <= end) {
                    int i = round - begin;
                    if (!bs.get(i) && pull(remote, round)) {
                        bs.set(i);
                        round++;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    private boolean pull(SocketChannel remote, int round) {
        try {
            ByteBuffer cmd = codec.encodePull(round);
            while (cmd.hasRemaining()) {
                remote.write(cmd);
            }
            Optional<Proposal> ret = codec.decodePull(remote);
            if (!ret.isPresent()) {
                return false;
            } else {
                proposalService.saveProposal(ret.get());
            }
            return true;
        } catch (Exception e) {
            // TODO
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