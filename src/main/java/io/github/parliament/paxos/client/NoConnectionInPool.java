package io.github.parliament.paxos.client;

import java.net.SocketAddress;

class NoConnectionInPool extends Exception {
	private static final long serialVersionUID = -8940904455982405523L;

	NoConnectionInPool(SocketAddress address) {
        super("no connection in pool for" + address);
    }
}
