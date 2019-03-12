package io.github.parliament.paxos;

public class DefaultProposalSeqNoGenerator implements ProposalSeqNoGenerator<String> {

    @Override
    public String next() {
        long m = System.currentTimeMillis();
        
        return null;
    }

}
