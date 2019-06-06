package io.github.parliament.paxos.client;

import io.github.parliament.paxos.acceptor.Acceptor;

import java.util.List;

public interface PeerAcceptors {
    List<? extends Acceptor> create(int round);

    void release(int round);
}
