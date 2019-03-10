/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2019 All Rights Reserved.
 */

import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.Proposer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

/**
 * 测试一个回合的提议过程
 * @author zy
 * @version $Id: PaxosTest.java, v 0.1 2019年03月08日 2:35 PM zy Exp $
 */
@ExtendWith(MockitoExtension.class)
class PaxosTest {
    @Mock
    private Proposer proposer;

    @InjectMocks
    private Paxos paxos;

    @Test
    void start() {
        Proposal proposal = new Proposal();
        paxos.start(proposal);
        verify(proposer, times(1)).propose(proposal);
    }
}