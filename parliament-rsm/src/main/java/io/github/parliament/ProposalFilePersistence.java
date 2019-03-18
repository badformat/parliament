package io.github.parliament;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import com.google.common.base.Preconditions;

import io.github.parliament.files.FileService;
import io.github.parliament.paxos.Acceptor;
import io.github.parliament.persistence.ProposalPersistenceServie;

public class ProposalFilePersistence implements ProposalPersistenceServie {
    private Path path;
    private FileService fileService;

    public ProposalFilePersistence(Path path, FileService fileservice) {
        this.path = path;
        this.fileService = fileservice;
    }

    @Override
    public void persistenceProposal(Proposal proposal) throws Exception {
        Path file = buildProposalFilePath(proposal.getRound());
        Preconditions.checkState(!fileService.exists(file), "该轮提案文件已存在");

        ByteBuffer bb = ByteBuffer.wrap(proposal.getContent());

        fileService.writeAll(file, bb);
    }

    @Override
    public void persistenceAcceptor(long round, Acceptor<?> acceptor) {
        // TODO Auto-generated method stub
    }

    @Override
    public Optional<Proposal> recoverProposal(long round) throws Exception {
        Path file = buildProposalFilePath(round);

        if (!fileService.exists(file)) {
            return Optional.empty();
        }
        byte[] content = fileService.readAll(file);
        return Optional.of(new Proposal(round, content));
    }

    @Override
    public Acceptor<?> recoverAcceptor(long round) {
        // TODO Auto-generated method stub
        return null;
    }

    private Path buildProposalFilePath(long seq) {
        return Paths.get(path.toString(), "proposal_" + seq);
    }
}
