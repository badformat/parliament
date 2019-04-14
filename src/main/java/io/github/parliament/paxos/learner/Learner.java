package io.github.parliament.paxos.learner;

import java.util.stream.Stream;

import io.github.parliament.paxos.Proposal;

/**
 *
 * @author zy
 */
public interface Learner {
    Stream<Proposal> learn(int from, int to);

    int min();

    int max();
}