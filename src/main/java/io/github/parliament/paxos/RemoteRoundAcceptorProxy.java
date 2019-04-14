package io.github.parliament.paxos;

import java.io.IOException;

import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;

/**
 *
 * @author zy
 */
public interface RemoteRoundAcceptorProxy<T extends Comparable<T>> {
    Prepare<T> delegatePrepare(int round, T n) throws IOException;

    Accept<T> delegateAccept(int round, T n, byte[] value) throws IOException;

    void delegateDecide(int round, byte[] agreement) throws Exception;
}