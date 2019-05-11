package io.github.parliament.rsm;

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

    private volatile ConcurrentHashMap<String, Lock> fileLocks = new ConcurrentHashMap<>();

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
        fileLocks.put(seqFile, new ReentrantLock());
        fileLocks.put(maxFile, new ReentrantLock());
        fileLocks.put(minFile, new ReentrantLock());
    }

    @Override
    public Optional<byte[]> getProposal(int round) throws Exception {
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

            return Optional.of(fileService.readAll(roundFile));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void saveProposal(int round, byte[] value) throws Exception {
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            Path roundFile = dataPath.resolve(String.valueOf(round));
            Optional<byte[]> p = getProposal(round);
            if (p.isPresent()) {
                Preconditions.checkState(Arrays.equals(p.get(), value));
                return;
            }

            fileService.createFileIfNotExists(roundFile);

            fileService.overwriteAll(roundFile, ByteBuffer.wrap(value));

            if (round > maxRound()) {
                updateMaxRound(round);
                updateNextRound(round);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void notice(int round, byte[] value) throws Exception {
        Preconditions.checkState(round >= minRound());
        Preconditions.checkNotNull(value, "va is null");
        Lock lock = getLockFor(round);
        try {
            lock.lock();
            saveProposal(round, value);
        } finally {
            lock.unlock();
        }
    }

    int nextRound() throws Exception {
        try {
            fileLocks.get(seqFile).lock();
            return getAndIncreaseIntFile(seqFile);
        } finally {
            fileLocks.get(seqFile).unlock();
        }
    }

    private void updateNextRound(int round) throws Exception {
        try {
            fileLocks.get(seqFile).lock();
            if (round > round()) {
                writeIntToFile(seqFile, round);
            }
        } finally {
            fileLocks.get(seqFile).unlock();
        }
    }

    @Override
    public int round() throws Exception {
        try {
            fileLocks.get(seqFile).lock();
            return readIntFromFile(seqFile);
        } finally {
            fileLocks.get(seqFile).unlock();
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
            fileLocks.get(minFile).lock();
            return readIntFromFile(minFile);
        } finally {
            fileLocks.get(minFile).unlock();
        }
    }

    @Override
    public void updateMaxRound(int max) throws Exception {
        try {
            fileLocks.get(maxFile).lock();
            if (max > maxRound()) {
                writeIntToFile(maxFile, max);
            }
        } finally {
            fileLocks.get(maxFile).unlock();
        }
    }

    @Override
    public int maxRound() throws Exception {
        try {
            fileLocks.get(maxFile).lock();
            return readIntFromFile(maxFile);
        } finally {
            fileLocks.get(maxFile).unlock();
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