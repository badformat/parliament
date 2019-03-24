package io.github.parliament;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.github.parliament.files.FileService;
import io.github.parliament.files.DefaultFileService;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.proposer.Sequence;
import io.github.parliament.persistence.ProposalPersistenceServie;
import lombok.Builder;
import lombok.Getter;

@Builder
public class ParliamentConf<T extends Serializable & Comparable<T>> {
    @Getter
    private AcceptorFactory<T> acceptorManager;

    @Getter
    private String dataDir;

    @Builder.Default
    @Getter
    private FileService fileservice = new DefaultFileService();

    @Getter
    private ProposalPersistenceServie proposalPersistenceService;

    @Builder.Default
    @Getter
    private ExecutorService executorService = Executors.newFixedThreadPool(100);

    @Getter
    private Sequence<T> sequence;
}
