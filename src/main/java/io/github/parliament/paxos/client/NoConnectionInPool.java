package io.github.parliament.paxos.client;

import java.net.SocketAddress;

class NoConnectionInPool extends Exception {
    NoConnectionInPool(SocketAddress address) {
        super("no connection in pool for" + address);
    }
}
