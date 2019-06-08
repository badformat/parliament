package io.github.parliament.paxos.client;

import io.github.parliament.paxos.acceptor.Acceptor;

import java.io.IOException;
import java.util.List;

public interface PeerAcceptors {
    List<? extends Acceptor> create(int round);

    void release(int round);

    int done() throws IOException;

    int max() throws IOException;
}
