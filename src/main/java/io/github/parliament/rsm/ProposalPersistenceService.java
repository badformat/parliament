package io.github.parliament.rsm;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import io.github.parliament.files.FileService;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import io.github.parliament.server.ProposalService;
import lombok.Builder;
import lombok.Getter;

/**
 *
 * @author zy
 */
public class ProposalPersistenceService implements ProposalService {
    private static final String maxFile = "max";
    private static final String minFile = "min";
    private static final String seqFile = "seq";

    private FileService fileService;

    @Getter
    private Path dataPath;

    private volatile ConcurrentMap<Integer, Lock> locks = new MapMaker()
            .weakValues()
            .makeMap();

    private volatile ConcurrentHashMap<String, Lock> fileLocsk = new ConcurrentHashMap<>();

    @Builder
    public ProposalPersistenceService(FileService fileService, Path path) throws Exception {
        this.fileService = fileService;
        this.dataPath = path;
        if (!fileService.exists(path)) {
            fileService.createDir(path);
        }
        createFileIfNotExists(path.resolve(seqFile));
        createFileIfNotExists(path.resolve(minFile));
        createFileIfNotExists(path.resolve(maxFile));
        fileLocsk.put(seqFile, new ReentrantLock());
        fileLocsk.put(maxFile, new ReentrantLock());
        fileLocsk.put(minFile, new ReentrantLock());
    }

    @Override
    public Optional<Proposal> getProposal(int round) throws Exception {
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            if (round < minRound()) {
                return Optional.empty();
            }
            Path roundFile = dataPath.resolve(String.valueOf(round));
            if (!fileService.exists(roundFile)) {
                return Optional.empty();
            }

            try (FileInputStream is = fileService.newInputStream(roundFile)) {
                ObjectInputStream ois = new ObjectInputStream(is);
                Proposal proposal = (Proposal) ois.readObject();
                return Optional.of(Proposal
                        .builder()
                        .round(proposal.getRound())
                        .agreement(proposal.getAgreement())
                        .build());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void saveProposal(Proposal proposal) throws Exception {
        Lock lock = getLockFor(proposal.getRound());
        try {
            lock.lock();
            int round = proposal.getRound();
            Path roundFile = dataPath.resolve(String.valueOf(round));
            Optional<Proposal> p = getProposal(round);
            if (p.isPresent()) {
                Preconditions.checkState(Arrays.equals(p.get().getAgreement(), proposal.getAgreement()));
                return;
            }

            fileService.createFileIfNotExists(roundFile);

            try (FileOutputStream os = fileService.newOutputstream(roundFile)) {
                ObjectOutputStream oos = new ObjectOutputStream(os);
                oos.writeObject(proposal);
            }
            if (round > maxRound()) {
                writeIntToFile(maxFile, round);
                updateSeq(round);
            }
        } finally {
            lock.unlock();
        }
    }

    void updateSeq(int seq) throws Exception {
        try {
            fileLocsk.get(seqFile).lock();
            writeIntToFile(seqFile, seq);
        } finally {
            fileLocsk.get(seqFile).unlock();
        }
    }

    @Override
    public void notice(int round, LocalAcceptor acceptor) throws Exception {
        Preconditions.checkState(round >= minRound());
        Preconditions.checkNotNull(acceptor.getVa(), "va is null");
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            Proposal proposal = Proposal.builder().round(round).agreement(acceptor.getVa()).build();
            saveProposal(proposal);
        } finally {
            lock.unlock();
        }
    }

    int nextRound() throws Exception {
        try {
            fileLocsk.get(seqFile).lock();
            return getAndIncreaseIntFile(seqFile);
        } finally {
            fileLocsk.get(seqFile).unlock();
        }
    }

    void forget(int round) throws Exception {
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            int min = minRound();
            if (round > min) {
                for (int i = min; i < round; i++) {
                    Path roundFile = dataPath.resolve(String.valueOf(i));
                    fileService.delete(roundFile);
                }
                writeIntToFile(minFile, round);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int minRound() throws Exception {
        try {
            fileLocsk.get(minFile).lock();
            return readIntFromFile(minFile);
        } finally {
            fileLocsk.get(minFile).unlock();
        }
    }

    @Override
    public void updateMaxRound(int max) throws Exception {
        try {
            fileLocsk.get(maxFile).lock();
            writeIntToFile(maxFile, max);
        } finally {
            fileLocsk.get(maxFile).unlock();
        }
    }

    @Override
    public int maxRound() throws Exception {
        try {
            fileLocsk.get(maxFile).lock();
            return readIntFromFile(maxFile);
        } finally {
            fileLocsk.get(maxFile).unlock();
        }
    }

    private void writeIntToFile(String fileName, int no) throws Exception {
        Path file = dataPath.resolve(fileName);
        fileService.overwriteAll(file, ByteBuffer.wrap(String.valueOf(no).getBytes()));
    }

    private int readIntFromFile(String name) throws Exception {
        Path file = dataPath.resolve(name);

        if (!fileService.exists(file)) {
            fileService.createFileIfNotExists(file);
            return -1;
        }
        byte[] bytes = fileService.readAll(file);
        if (bytes == null || bytes.length == 0) {
            return -1;
        }
        return Integer.valueOf(new String(bytes));
    }

    private int getAndIncreaseIntFile(String name) throws Exception {
        int i = readIntFromFile(name) + 1;
        Path file = dataPath.resolve(name);
        fileService.overwriteAll(file, ByteBuffer.wrap(String.valueOf(i).getBytes()));
        return i;
    }

    private void createFileIfNotExists(Path file) throws Exception {
        if (!fileService.exists(file)) {
            fileService.createFileIfNotExists(file);
        }
    }

    private Lock getLockFor(int round) {
        return locks.computeIfAbsent(round, (r) -> new ReentrantLock());
    }
}