package io.github.parliament.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;

public class PaxosAcceptorServer<T extends Comparable<T>> implements PaxosAcceptorServerInterface<T> {
	private ServerSocketChannel ssc;
	private Selector selector;

	@Override
	public Prepare<T> prepare(int round, T n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Accept<T> accept(int round, T n, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void decided(int round, byte[] agreement) {
		// TODO Auto-generated method stub

	}

	public void start(String host, int port) throws IOException {
		ssc = ServerSocketChannel.open();
		InetSocketAddress local = new InetSocketAddress(host, port);
		ssc.bind(local);
		ssc.configureBlocking(false);

		Selector selector = Selector.open();

		ssc.register(selector, SelectionKey.OP_ACCEPT);
		
		selector.select();
	}

}
