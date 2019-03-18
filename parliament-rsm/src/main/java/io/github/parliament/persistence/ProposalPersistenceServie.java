package io.github.parliament.persistence;

import java.util.Optional;

import io.github.parliament.Proposal;
import io.github.parliament.paxos.Acceptor;

public interface ProposalPersistenceServie {
    void persistenceProposal(Proposal proposal) throws Exception;

    void persistenceAcceptor(long round, Acceptor<?> acceptor);

    Optional<Proposal> recoverProposal(long round) throws Exception;

    Acceptor<?> recoverAcceptor(long round);
}
