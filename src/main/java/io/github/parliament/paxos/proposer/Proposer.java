package io.github.parliament.paxos.proposer;

import com.google.common.base.Preconditions;
import io.github.parliament.Sequence;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * @author zy
 */
public class Proposer {
    private static final Logger logger = LoggerFactory.getLogger(Proposer.class);
    private List<? extends Acceptor> acceptors;
    private int majority = Integer.MAX_VALUE;
    private Sequence<String> sequence;
    private boolean decided = false;
    private String n;
    private byte[] agreement;

    public Proposer(List<? extends Acceptor> acceptors, Sequence<String> sequence, final byte[] proposal) {
        Preconditions.checkArgument(proposal != null);
        this.acceptors = acceptors;
        this.majority = calcMajority(acceptors.size());
        this.sequence = sequence;
        this.agreement = proposal;
    }

    public byte[] propose(Consumer<Boolean> callback) {
        try {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            int retried = 0;
            while (!decided) {
                n = sequence.next();
                if (prepare()) {
                    decided = accept();
                }
                if (!decided) {
                    retried++;
                    try {
                        Thread.sleep(Math.abs(random.nextInt()) % 100);
                    } catch (InterruptedException e) {
                        logger.error("failed in propose.", e);
                        return null;
                    }
                    if (retried > 10) {
                        logger.error("failed in propose.Retried {} times.", 10);
                        throw new IllegalStateException();
                    }
                }
            }

            int decideCnt = 0;
            Preconditions.checkNotNull(agreement);
            for (Acceptor acceptor : acceptors) {
                try {
                    acceptor.decide(agreement);
                    decideCnt++;
                } catch (Exception e) {
                    logger.error("failed in propose.", e);
                }
            }

            if (decideCnt >= getMajority()) {
                return agreement;
            } else {
                logger.error("failed in propose. decided < majority.");
                decided = false;
                throw new IllegalStateException("decided < majority.");
            }
        } finally {
            callback.accept(decided);
        }
    }

    public boolean isDecided() {
        return decided;
    }

    boolean prepare() {
        List<Prepare> prepares = new ArrayList<>();
        int failedPeers = 0;

        for (Acceptor acceptor : acceptors) {
            Prepare prepare = null;
            try {
                prepare = acceptor.prepare(n);
            } catch (Exception e) {
                failedPeers++;
                logger.warn("failed in propose.", e);
                continue;
            }
            if (failedPeers >= getMajority()) {
                logger.error("prepared < majority.");
                throw new IllegalStateException("prepared < majority");
            }
            checkPrepare(acceptor, prepare);
            prepares.add(prepare);
        }

        int ok = 0;
        String max = null;
        for (Prepare prepare : prepares) {
            if (!prepare.isOk()) {
                continue;
            }
            ok++;
            String na = prepare.getNa();
            if (na != null) {
                if (max == null) {
                    max = na;
                }
                if (na.compareTo(max) >= 0) {
                    max = na;
                    Preconditions.checkNotNull(prepare.getVa());
                    agreement = prepare.getVa();
                }
            }
        }

        return ok >= majority;
    }

    private void checkPrepare(Acceptor acceptor, Prepare prepare) {
        if (prepare.isOk()) {
            Preconditions.checkState(Objects.equals(prepare.getN(), n),
                    acceptor + " prepare请求返回序号为" + prepare.getN() + "，应该是" + n);
        }
    }

    boolean accept() {
        int ok = 0;
        int failedPeers = 0;
        Preconditions.checkNotNull(agreement);

        for (Acceptor acceptor : acceptors) {
            Accept accept = null;
            try {
                accept = acceptor.accept(n, agreement);
            } catch (Exception e) {
                failedPeers++;
                logger.info("failed in accept", e);
                continue;
            }

            if (failedPeers >= getMajority()) {
                logger.info("failed in accept. accepted < majority");
                throw new IllegalStateException("accepted < majority");
            }

            checkAccept(acceptor, accept);
            if (accept.isOk()) {
                ok++;
            }
        }
        return ok >= majority;
    }

    private void checkAccept(Acceptor acceptor, Accept accept) {
        if (accept.isOk()) {
            Preconditions.checkState(Objects.deepEquals(accept.getN(), this.n),
                    acceptor + "返回的提案编号为" + accept.getN() + "，应该是" + this.n);
        }
    }

    int getMajority() {
        return this.majority;
    }

    int calcMajority(int size) {
        return (int) Math.ceil((size + 1) / 2.0d);
    }

    void setN(String n) {
        this.n = n;
    }

    byte[] getAgreement() {
        return this.agreement;
    }

}