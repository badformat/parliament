package io.github.parliament.paxos.server;

import io.github.parliament.Coordinator;
import io.github.parliament.paxos.acceptor.LocalAcceptors;
import lombok.Builder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zy
 */
public class PaxosSyncServer implements Runnable {
    private volatile boolean started = false;
    private volatile InetSocketAddress me;
    private volatile ServerSocketChannel ssc;
    private volatile Thread selectorThread;
    private volatile ExecutorService executorService;
    private volatile LocalAcceptors localAcceptors;
    private volatile Coordinator coordinator;
    private volatile int peersNo;
    private volatile List<SocketChannel> clients;

    @Builder
    public PaxosSyncServer(InetSocketAddress me, LocalAcceptors localAcceptors, Coordinator coordinator,
                           int peersNo) {
        this.me = me;
        this.localAcceptors = localAcceptors;
        this.coordinator = coordinator;
        this.peersNo = peersNo;
        this.clients = Collections.synchronizedList(new ArrayList<>());
    }

    public void start() throws IOException {
        if (started) {
            throw new IllegalStateException();
        }
        ssc = ServerSocketChannel.open();
        ssc.bind(me);
        ssc.configureBlocking(true);
        ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);

        executorService = Executors.newFixedThreadPool(peersNo * 2);
        started = true;
        selectorThread = new Thread(this);
        selectorThread.start();
    }

    public void shutdown() throws IOException {
        if (!started) {
            return;
        }
        executorService.shutdown();
        selectorThread.interrupt();
        ssc.close();
        started = false;
        for (SocketChannel c : clients) {
            c.close();
        }
        clients.clear();
    }

    @Override
    public void run() {
        while (started) {
            try {
                if (!ssc.isOpen()) {
                    ssc = ServerSocketChannel.open();
                    ssc.bind(me);
                    ssc.configureBlocking(true);
                    ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                }
                SocketChannel cc = ssc.accept();
                clients.add(cc);
                RemotePeerHandler h = new RemotePeerHandler(cc, localAcceptors, coordinator);
                executorService.submit(h);
            } catch (IOException e) {
                //TODO
                //e.printStackTrace();
            }
        }
    }
}