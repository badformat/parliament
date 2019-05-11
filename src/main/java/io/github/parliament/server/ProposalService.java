package io.github.parliament.server;

import java.util.Optional;

/**
 *
 * @author zy
 */
public interface ProposalService {
    Optional<byte[]> getProposal(int round) throws Exception;

    void saveProposal(int round, byte[] value) throws Exception;

    void notice(int round, byte[] value) throws Exception;

    int maxRound() throws Exception;

    int minRound() throws Exception;

    int round() throws Exception;

    void updateMaxRound(int max) throws Exception;
}