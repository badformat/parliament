package io.github.parliament.persistence;

import java.util.Optional;

import io.github.parliament.Proposal;

public interface ProposalPersistenceServie {
    void saveProposal(Proposal proposal) throws Exception;

    Optional<Proposal> getProposal(long round) throws Exception;
}
