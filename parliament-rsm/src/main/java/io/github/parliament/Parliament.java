package io.github.parliament;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Optional;

import com.google.common.base.Preconditions;

import io.github.parliament.paxos.Acceptor;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.Proposer;
import io.github.parliament.paxos.Sequence;

public class Parliament<T extends Comparable<T>> implements Observer {
    private static final Charset ASCII = Charset.forName("US-ASCII");

    private ParliamentConf<T> config;
    private List<Acceptor<T>> acceptors;
    private FileService fileService;
    private Path path;
    private Path seqFilePath;
    private Sequence<T> sequence;

    public Parliament(ParliamentConf<T> conf) {
        this.config = conf;
        this.acceptors = conf.getAcceptors();
        this.fileService = conf.getFileService();
        this.path = Paths.get(config.getDataDir(), "local");
        this.seqFilePath = Paths.get(path.toString(), "seq");
        this.sequence = config.getSequence();
    }

    public void start() throws Exception {
        fileService.createDir(path);
        fileService.createFile(seqFilePath);
    }

    public void shutdown() {
    }

    public Proposal propose(byte[] bytes) throws Exception {
        long seq = seq();
        Proposal proposal = new Proposal(seq, bytes);
        persistence(proposal);

        Proposer<T> proposer = new Proposer<T>(acceptors, sequence, proposal);
        proposer.propose();
        return proposal;
    }

    @Override
    public void update(Observable proposal, Object arg) {

    }

    synchronized long seq() throws Exception {
        byte[] bytes = fileService.readAll(seqFilePath);
        long seq = 0L;
        if (bytes.length != 0) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            seq = Long.valueOf(ASCII.decode(bb).toString());
        }
        fileService.overwriteAll(seqFilePath, ASCII.encode(String.valueOf(seq + 1)));
        return seq;
    }

    void persistence(Proposal proposal) throws Exception {
        Path file = buildProposalFilePath(proposal.getRound());
        Preconditions.checkState(!fileService.exists(file), "该轮次提案文件已存在");

        ByteBuffer bb = ByteBuffer.wrap(proposal.getContent());

        fileService.writeAll(file, bb);
    }

    Optional<Proposal> regainProposal(long seq) throws Exception {
        Path file = buildProposalFilePath(seq);

        if (!fileService.exists(file)) {
            return Optional.empty();
        }
        byte[] content = fileService.readAll(file);
        return Optional.of(new Proposal(seq, content));
    }

    private Path buildProposalFilePath(long seq) {
        return Paths.get(path.toString(), "proposal_" + seq);
    }
}
