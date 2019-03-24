package io.github.parliament;

import java.util.concurrent.Callable;

import io.github.parliament.paxos.proposer.Proposer;

public class ProposeCallable implements Callable<Proposal> {
    private Proposer<?> proposer;
    private Proposal proposal;

    public ProposeCallable(Proposer<?> proposer, Proposal proposal) {
        this.proposer = proposer;
        this.proposal = proposal;
    }

    @Override
    public Proposal call() throws Exception {
        proposal.setAgreement(proposer.propose());
        return proposal;
    }

}
