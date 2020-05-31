package io.github.parliament.paxos.client;

import java.util.List;

public interface PeerAcceptors {
    List<SyncProxyAcceptor> create(int round);

    void release(int round);
}
