package io.github.parliament.paxos.acceptor;

/**
 * @author zy
 */
public interface Acceptor {
    Prepare prepare(String n) throws Exception;

    Accept accept(String n, byte[] value) throws Exception;

    void decide(byte[] agreement) throws Exception;
}
