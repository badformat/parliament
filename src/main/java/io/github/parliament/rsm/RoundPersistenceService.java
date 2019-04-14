package io.github.parliament.rsm;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import io.github.parliament.files.FileService;
import io.github.parliament.paxos.Proposal;
import lombok.Builder;
import lombok.Getter;

/**
 *
 * @author zy
 */
public class RoundPersistenceService implements AgreementListener {
    private FileService fileService;

    @Getter
    private Path dataPath;

    private volatile ConcurrentMap<Integer, Lock> locks = new MapMaker()
            .weakValues()
            .makeMap();



    @Builder
    public RoundPersistenceService(FileService fileService, Path path) throws Exception {
        this.fileService = fileService;
        this.dataPath = path;
        if (!fileService.exists(path)) {
            fileService.createDir(path);
        }
        createFileIfNotExists(path.resolve("seq"));
        createFileIfNotExists(path.resolve("min"));
        createFileIfNotExists(path.resolve("max"));
    }

    Optional<RoundLocalAcceptor> getAcceptorFor(int round) throws Exception {
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            Path roundFile = dataPath.resolve(String.valueOf(round));
            if (!fileService.exists(roundFile)) {
                return Optional.empty();
            }

            try (FileInputStream is = fileService.newInputStream(roundFile)) {
                ObjectInputStream ois = new ObjectInputStream(is);
                RoundLocalAcceptor acceptor = (RoundLocalAcceptor) ois.readObject();
                acceptor.setReachedAgreement(this);
                return Optional.of(acceptor);
            }
        } finally {
            lock.unlock();
        }
    }

    void saveAcceptor(RoundLocalAcceptor acceptor) throws Exception {
        Lock lock = getLockFor(acceptor.getRound());
        try {
            lock.lock();
            int round = acceptor.getRound();
            Path roundFile = dataPath.resolve(String.valueOf(round));
            Optional<RoundLocalAcceptor> acc = getAcceptorFor(round);
            if (acc.isPresent()) {
                Preconditions.checkState(Objects.equals(acc.get().getReachedAgreement(), acceptor.getReachedAgreement()));
                return;
            }

            fileService.createFileIfNotExists(roundFile);

            try (FileOutputStream os = fileService.newOutputstream(roundFile)) {
                ObjectOutputStream oos = new ObjectOutputStream(os);
                oos.writeObject(acceptor);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void notice(RoundLocalAcceptor acceptor) throws Exception {
        int round = acceptor.getRound();
        Preconditions.checkState(round >= minRound());
        Preconditions.checkNotNull(acceptor.getVa(), "va is null");
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            saveAcceptor(acceptor);
            int maxRound = maxRound();
            if (acceptor.getRound() > maxRound) {
                Path file = dataPath.resolve("max");
                fileService.overwriteAll(file, ByteBuffer.wrap(String.valueOf(acceptor.getRound()).getBytes()));
            }
        } finally {
            lock.unlock();
        }
    }

    synchronized int nextRound() throws Exception {
        return getAndIncreaseIntFile("seq");
    }

    void forgetRoundsTo(int round) throws Exception {
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            int min = minRound();
            if (round > min) {
                for (int i = min; i < round; i++) {
                    Path roundFile = dataPath.resolve(String.valueOf(round));
                    fileService.delete(roundFile);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    synchronized int minRound() throws Exception {
        return readIntFromFile("min");
    }

    synchronized void setMin(int min) throws Exception {
        Path file = dataPath.resolve("min");
        fileService.overwriteAll(file, ByteBuffer.wrap(String.valueOf(min).getBytes()));
    }

    synchronized int maxRound() throws Exception {
        return readIntFromFile("max");
    }

    Optional<Proposal> getRound(int round) throws Exception {
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            Optional<RoundLocalAcceptor> acc = getAcceptorFor(round);
            return acc.map(acceptor -> Proposal.builder().round(round).agreement(acceptor.getVa()).build());
        } finally {
            lock.unlock();
        }
    }

    private int readIntFromFile(String name) throws Exception {
        Path file = dataPath.resolve(name);

        if (!fileService.exists(file)) {
            fileService.createFileIfNotExists(file);
            return 0;
        }
        byte[] bytes = fileService.readAll(file);
        if (bytes == null || bytes.length == 0) {
            return 0;
        }
        return Integer.valueOf(new String(bytes));
    }

    synchronized private int getAndIncreaseIntFile(String name) throws Exception {
        //fileService.lock(seqFile)
        // TODO lock file
        Path file = dataPath.resolve(name);
        byte[] bytes = fileService.readAll(file);
        int i;
        if (bytes == null || bytes.length == 0) {
            i = 0;
        } else {
            i = Integer.valueOf(new String(bytes));
        }
        fileService.overwriteAll(file, ByteBuffer.wrap(String.valueOf(i + 1).getBytes()));
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