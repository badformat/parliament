package io.github.parliament.server;

import java.util.Optional;

import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.acceptor.LocalAcceptor;

/**
 *
 * @author zy
 */
public interface ProposalService {
    Optional<Proposal> getProposal(int round) throws Exception;

    void saveProposal(Proposal event) throws Exception;

    void notice(int round, LocalAcceptor acceptor) throws Exception;

    int maxRound() throws Exception;

    int minRound() throws Exception;
}