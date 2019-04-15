package io.github.parliament.rsm;

import io.github.parliament.server.ProposalService;
import io.github.parliament.paxos.acceptor.LocalAcceptor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author zy
 */
@Builder
class RoundLocalAcceptor extends LocalAcceptor<String> {
    @Getter
    private int round;

    @Getter
    @Setter
    transient private ProposalService proposalService;

    @Override
    public void decide(byte[] agreement) throws Exception {
        proposalService.notice(round, this);
    }
}