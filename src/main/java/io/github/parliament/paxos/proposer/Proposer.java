package io.github.parliament.paxos.proposer;

import com.google.common.base.Preconditions;
import io.github.parliament.Sequence;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * @author zy
 */
public class Proposer {
    private static final Logger logger = LoggerFactory.getLogger(Proposer.class);
    private List<? extends Acceptor> acceptors;
    private int quorum = Integer.MAX_VALUE;
    private Sequence<String> sequence;
    private boolean decided = false;
    private String n;
    private byte[] agreement;

    public Proposer(List<? extends Acceptor> acceptors, Sequence<String> sequence, final byte[] proposal) {
        Preconditions.checkArgument(proposal != null);
        this.acceptors = acceptors;
        this.quorum = quorum(acceptors.size());
        this.sequence = sequence;
        this.agreement = proposal;
    }

    public byte[] propose(Consumer<Boolean> callback) {
        try {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            int retryCount = 0;
            while (!decided) {
                n = sequence.next();
                if (prepare()) {
                    decided = accept();
                    if (!decided) {
                        logger.debug("accept rejected by quorum.");
                    }
                } else {
                    logger.debug("prepare rejected by quorum.");
                }
                if (!decided) {
                    retryCount++;
                    try {
                        Thread.sleep(Math.abs(random.nextInt()) % 300);
                    } catch (InterruptedException e) {
                        logger.error("Failed in propose.", e);
                        return null;
                    }
                    if (retryCount > 3) {
                        logger.error("Failed in propose.Retried {} times.", 3);
                        throw new IllegalStateException();
                    }
                }
            }

            Preconditions.checkNotNull(agreement);

            Optional<Integer> success = acceptors.stream().parallel().map(acceptor -> {
                try {
                    acceptor.decide(agreement);
                    return 1;
                } catch (Exception e) {
                    logger.error("failed in decide().", e);
                    return 0;
                }
            }).reduce((i, j) -> j + j);

            if (success.orElse(0) >= getQuorum()) {
                return agreement;
            } else {
                logger.error("failed in propose. decided < quorum.");
                decided = false;
                throw new IllegalStateException("decided < quorum.");
            }
        } finally {
            callback.accept(decided);
        }
    }

    boolean isDecided() {
        return decided;
    }

    boolean prepare() {
        List<Prepare> prepares = Collections.synchronizedList(new ArrayList<>());

        Optional<Integer> success = acceptors.stream().parallel().map(acceptor -> {
            try {
                Prepare prepare = acceptor.prepare(n);
                checkPrepare(acceptor, prepare);
                prepares.add(prepare);
                return 1;
            } catch (Exception e) {
                logger.warn("failed in propose.", e);
                return 0;
            }
        }).reduce((p, n) -> p + n);

        if (success.orElse(0) < getQuorum()) {
            logger.error("success prepare() < quorum. ");
            throw new IllegalStateException("success prepare() < quorum");
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

        return ok >= quorum;
    }

    private void checkPrepare(Acceptor acceptor, Prepare prepare) {
        if (prepare.isOk()) {
            Preconditions.checkState(Objects.equals(prepare.getN(), n),
                    acceptor + " prepare请求返回序号为" + prepare.getN() + "，应该是" + n);
        }
    }

    boolean accept() {
        Preconditions.checkNotNull(agreement);

        Optional<Integer> success = acceptors.stream().parallel().map(acceptor -> {
            try {
                Accept accept = acceptor.accept(n, agreement);
                checkAccept(acceptor, accept);
                return accept.isOk() ? 1 : 0;
            } catch (Exception e) {
                logger.info("failed in accept", e);
                return 0;
            }
        }).reduce((n, p) -> n + p);

        return success.orElse(0) >= getQuorum();
    }

    private void checkAccept(Acceptor acceptor, Accept accept) {
        if (accept.isOk()) {
            Preconditions.checkState(Objects.deepEquals(accept.getN(), this.n),
                    acceptor + "返回的提案编号为" + accept.getN() + "，应该是" + this.n);
        }
    }

    int getQuorum() {
        return this.quorum;
    }

    int quorum(int size) {
        return (int) Math.ceil((size + 1) / 2.0d);
    }

    void setN(String n) {
        this.n = n;
    }

    byte[] getAgreement() {
        return this.agreement;
    }

}