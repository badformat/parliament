package io.github.parliament.server;

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

import io.github.parliament.paxos.acceptor.AcceptorFactory;
import lombok.Builder;

/**
 *
 * @author zy
 */
public class PaxosSyncServer implements Runnable {
    private volatile boolean                 started = false;
    private volatile InetSocketAddress       me;
    private volatile ServerSocketChannel     ssc;
    private volatile Thread                  selectorThread;
    private volatile ExecutorService         executorService;
    private volatile AcceptorFactory<String> acceptorFactory;
    private volatile ProposalService         proposalService;
    private volatile int                     peersNo;
    private volatile List<SocketChannel>     clients;

    @Builder
    public PaxosSyncServer(InetSocketAddress me, AcceptorFactory<String> acceptorFactory, ProposalService proposalService,
                           int peersNo) {
        this.me = me;
        this.acceptorFactory = acceptorFactory;
        this.proposalService = proposalService;
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
                RemotePeerHandler h = new RemotePeerHandler(cc, acceptorFactory, proposalService);
                executorService.submit(h);
            } catch (IOException e) {
                //TODO
                //e.printStackTrace();
            }
        }
    }
}