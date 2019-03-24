package io.github.parliament;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import io.github.parliament.files.FileService;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.proposer.Proposer;
import io.github.parliament.paxos.proposer.Sequence;
import io.github.parliament.persistence.ProposalPersistenceServie;

public class Parliament<T extends Serializable & Comparable<T>> {
    private static final Charset ASCII = Charset.forName("US-ASCII");

    private ParliamentConf<T> config;
//    private AcceptorFactory<T> acceptorFactorty;
    private ExecutorService executorService;
    private Path roundFilePath;
    private Sequence<T> sequence;
    private FileService fileService;
    private ProposalPersistenceServie proposalPersistenceServie;

    public Parliament(ParliamentConf<T> conf) { // TODO replace conf with inject
        this.config = conf;
//        this.acceptorFactorty = config.getAcceptorManager();
        this.roundFilePath = Paths.get(config.getDataDir().toString(), "round");
        this.sequence = config.getSequence();
        this.executorService = config.getExecutorService();
        this.fileService = config.getFileservice();
        this.proposalPersistenceServie = config.getProposalPersistenceService();
    }

    public void start() throws Exception {
        fileService.createDir(Paths.get(config.getDataDir()));
        fileService.createFile(roundFilePath);
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public Future<Proposal> propose(byte[] bytes) throws Exception {
        int round = round();
        Proposal proposal = new Proposal(round, bytes);
        proposalPersistenceServie.saveProposal(proposal);

        return propose(proposal);
    }

    public Optional<Proposal> propoal(long round) throws Exception {
        return this.proposalPersistenceServie.getProposal(round);
    }

    public Iterator<Proposal> proposals(long begin) {
        return null;
    }

    public Collection<Proposal> proposals(long begin, long end) {
        return null;
    }

    Future<Proposal> propose(Proposal proposal) throws Exception {
//        Collection<Acceptor<T>> acceptors = acceptorFactorty.createPeersForRound(proposal.getRound());
//        Proposer<T> proposer = new Proposer<T>(acceptors, sequence, proposal.getContent());

//        return executorService.submit(new ProposeCallable(proposer, proposal));
        return null;
    }

    synchronized int round() throws Exception {
        byte[] bytes = fileService.readAll(roundFilePath);
        int seq = 0;
        if (bytes.length != 0) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            seq = Integer.valueOf(ASCII.decode(bb).toString());
        }
        fileService.overwriteAll(roundFilePath, ASCII.encode(String.valueOf(seq + 1)));
        return seq;
    }
}
