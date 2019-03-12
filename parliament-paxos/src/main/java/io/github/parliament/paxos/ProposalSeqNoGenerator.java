package io.github.parliament.paxos;

public interface ProposalSeqNoGenerator<T extends Comparable<?>> {
    T next();
}
