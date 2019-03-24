package io.github.parliament.paxos.proposer;

public interface ProposerFactory<T extends Comparable<T>> {
    Proposer<T> createProposerForRound(int round, byte[] bytes);
}
