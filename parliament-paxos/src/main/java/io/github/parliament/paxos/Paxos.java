package io.github.parliament.paxos;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.inject.Inject;

import io.github.parliament.paxos.proposer.Proposer;
import io.github.parliament.paxos.proposer.ProposerFactory;

public class Paxos {
    @Inject
    private ProposerFactory<?> proposerFactory;
    @Inject
    private ExecutorService executorService;

    public void shutdown() {
        executorService.shutdown();
    }

    public Future<byte[]> propose(int round, byte[] value) {
        Proposer<?> proposer = proposerFactory.createProposerForRound(round, value);
        return executorService.submit(new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return proposer.propose();
            }
        });
    }
}
