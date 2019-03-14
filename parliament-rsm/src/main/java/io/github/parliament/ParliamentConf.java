package io.github.parliament;

import java.util.Collections;
import java.util.List;

import io.github.parliament.paxos.Acceptor;
import io.github.parliament.paxos.Sequence;
import lombok.Builder;
import lombok.Singular;

@Builder
public class ParliamentConf<T extends Comparable<T>> {
    @Singular
    private List<Acceptor<T>> acceptors;
    private String dataDir;
    @Builder.Default
    private FileService fileService = new DefaultFileService();
    private Sequence<T> sequence;

    public List<Acceptor<T>> getAcceptors() {
        return Collections.unmodifiableList(acceptors);
    }

    public String getDataDir() {
        return this.dataDir;
    }

    public FileService getFileService() {
        return fileService;
    }

    public Sequence<T> getSequence() {
        return sequence;
    }
}
