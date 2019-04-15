package io.github.parliament.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;

import io.github.parliament.paxos.acceptor.AcceptorFactory;
import lombok.Builder;

/**
 *
 * @author zy
 */
public class PaxosSyncServer implements Runnable {
    enum Status {
        init,
        started,
        stopped
    }

    private volatile Status status = Status.init;

    private volatile InetSocketAddress       me;
    private volatile ServerSocketChannel     ssc;
    private          Thread                  selectorThread;
    private          ExecutorService         executorService;
    private volatile AcceptorFactory<String> acceptorFactory;
    private volatile ProposalService         proposalService;

    @Builder
    public PaxosSyncServer(InetSocketAddress me, AcceptorFactory<String> acceptorFactory, ProposalService proposalService,
                           ExecutorService executorService) {
        this.me = me;
        this.acceptorFactory = acceptorFactory;
        this.executorService = executorService;
        this.proposalService = proposalService;
    }

    public void start() throws IOException {
        if (status != Status.init) {
            throw new IllegalStateException();
        }

        ssc = ServerSocketChannel.open();
        ssc.bind(me);
        ssc.configureBlocking(true);
        ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);

        selectorThread = new Thread(this);
        selectorThread.start();
        status = Status.started;
    }

    public void shutdown() throws Exception {
        if (status != Status.started) {
            return;
        }
        executorService.shutdown();
        selectorThread.interrupt();
        status = Status.stopped;
    }

    @Override
    public void run() {
        try {
            while (status != Status.stopped) {
                SocketChannel cc = ssc.accept();
                RemotePeerHandler h = new RemotePeerHandler(cc, acceptorFactory, proposalService);
                executorService.submit(h);
            }
        } catch (IOException e) {
            //TODO
            //e.printStackTrace();
        }
    }
}