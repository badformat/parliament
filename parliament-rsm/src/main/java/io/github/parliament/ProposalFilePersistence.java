package io.github.parliament;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import com.google.common.base.Preconditions;

import io.github.parliament.files.FileService;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.persistence.ProposalPersistenceServie;

public class ProposalFilePersistence implements ProposalPersistenceServie {
    private Path path;
    private FileService fileService;

    public ProposalFilePersistence(Path path, FileService fileservice) throws Exception {
        this.path = path;
        this.fileService = fileservice;
        this.fileService.createDir(path);
    }

    @Override
    public void saveProposal(Proposal proposal) throws Exception {
        Path file = buildProposalFilePath(proposal.getRound());
        Preconditions.checkState(!fileService.exists(file), "该轮提案文件已存在");
        // TODO lock file
        fileService.createFile(file);
        try (ObjectOutputStream os = new ObjectOutputStream(Files.newOutputStream(file, StandardOpenOption.WRITE))) {
            os.writeObject(proposal);
        }
    }

    @Override
    public Optional<Proposal> getProposal(long round) throws Exception {
        Path file = buildProposalFilePath(round);
        if (!fileService.exists(file)) {
            return Optional.empty();
        }

        try (ObjectInputStream is = new ObjectInputStream(Files.newInputStream(file))) {
            return Optional.of((Proposal) is.readObject());
        }
    }
//
//    @Override
//    public void saveAcceptor(long round, Acceptor<?> acceptor) throws Exception {
//        Path proposalfile = buildProposalFilePath(round);
//        Preconditions.checkState(fileService.exists(proposalfile), "该轮提案文件还不存在");
//
//        Path acceptorFile = buildAcceptorFilePath(round);
//        if (!fileService.exists(acceptorFile)) {
//            fileService.createFile(acceptorFile);
//        }
//
//        try (ObjectOutputStream os = new ObjectOutputStream(
//                Files.newOutputStream(acceptorFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING))) {
//            os.writeObject(acceptor);
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    @Override
//    public <T extends Serializable & Comparable<T>> Acceptor<T> getAcceptor(long round) throws Exception {
//        Path file = buildAcceptorFilePath(round);
//        Preconditions.checkState(fileService.exists(file), "该轮acceptor文件还不存在");
//
//        try (ObjectInputStream is = new ObjectInputStream(Files.newInputStream(file))) {
//            return (Acceptor<T>) is.readObject();
//        }
//    }

    private Path buildProposalFilePath(long round) {
        return Paths.get(path.toString(), "proposal_" + round);
    }

    private Path buildAcceptorFilePath(long round) {
        return Paths.get(path.toString(), "acceptor_" + round);
    }
}
