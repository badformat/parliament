package io.github.parliament.rsm;

import java.io.Serializable;

import io.github.parliament.paxos.acceptor.LocalAcceptor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author zy
 */
@Builder
class RoundLocalAcceptor extends LocalAcceptor<String> implements Serializable {
    public static final long serialVersionUID = 1L;
    @Getter
    private             int  round;

    @Getter
    @Setter
    transient private ReachedAgreement reachedAgreement;

    @Override
    public void decide(byte[] agreement) throws Exception {
        reachedAgreement.notice(this);
    }
}