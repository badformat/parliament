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
    private int quorum;
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
                        int s = random.nextInt();
                        Thread.sleep((s == Integer.MIN_VALUE ? 10: Math.abs(s)) % 300);
                    } catch (InterruptedException e) {
                        logger.error("Paxos提案失败", e);
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("线程中断");
                    }
                    if (retryCount >= 7) {
                        logger.error("Paxos提案失败，已重试{}次，放弃提案", 7);
                        throw new IllegalStateException("Paxos提案重试大于最大次数");
                    }
                }
            }

            Preconditions.checkNotNull(agreement);

            Optional<Integer> success = acceptors.stream().parallel().map(acceptor -> {
                try {
                    acceptor.decide(agreement);
                    return 1;
                } catch (Exception e) {
                    logger.error("decide()失败", e);
                    return 0;
                }
            }).reduce((i, j) -> j + j);

            if (success.orElse(0) >= getQuorum()) {
                return agreement;
            } else {
                logger.error("Paxos提案失败，未获得多数票");
                decided = false;
                throw new IllegalStateException("Paxos提案失败，未获得多数票");
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
                logger.warn("Paxos提案失败", e);
                return 0;
            }
        }).reduce((s, p) -> s + p);

        if (success.orElse(0) < getQuorum()) {
            logger.error("prepare未获得多数票");
            throw new IllegalStateException("prepare未获得多数票");
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
                     "%s prepare请求返回序号为%s，期望是%s",acceptor, prepare.getN(), n);
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
                logger.info("accept失败", e);
                return 0;
            }
        }).reduce((s, p) -> s + p);

        return success.orElse(0) >= getQuorum();
    }

    private void checkAccept(Acceptor acceptor, Accept accept) {
        if (accept.isOk()) {
            Preconditions.checkState(Objects.deepEquals(accept.getN(), this.n),
                    "返回的提案编号为%s，期望是%s", acceptor, accept.getN(), this.n);
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