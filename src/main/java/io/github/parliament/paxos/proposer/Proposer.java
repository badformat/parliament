package io.github.parliament.paxos.proposer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.Prepare;

/**
 *
 * @author zy
 */
public class Proposer<T extends Comparable<T>> {
    private Collection<Acceptor<T>> acceptors;
    private int                     majority = Integer.MAX_VALUE;
    private Sequence<T>             sequence;
    private boolean                 decided  = false;
    private T                       n;
    private byte[]                  agreement;

    public Proposer(Collection<Acceptor<T>> acceptors, Sequence<T> sequence,final byte[] proposal) {
        Preconditions.checkArgument(proposal != null);
        this.acceptors = acceptors;
        this.majority = calcMajority(acceptors.size());
        this.sequence = sequence;
        this.agreement = proposal;
    }

    public byte[] propose() throws InterruptedException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int retried = 0;
        while (!decided) {
            n = sequence.next();
            if (prepare()) {
                decided = accept();
            }
            retried++;
            Thread.sleep(Math.abs(random.nextInt()) % 100);
            if (retried > 10) {
                // TODO
                throw new IllegalStateException();
            }
        }

        int decideCnt = 0;
        Preconditions.checkNotNull(agreement);
        for (Acceptor<T> acceptor : acceptors) {
            try {
                acceptor.decide(agreement);
                decideCnt++;
            } catch (Exception e) {
                //TODO
                e.printStackTrace();
            }
        }

        if (decideCnt >= getMajority()) {
            return agreement;
        } else {
            //TODO
            decided = false;
            throw new IllegalStateException("");
        }
    }

    public boolean isDecided() {
        return decided;
    }

    boolean prepare() {
        List<Prepare<T>> prepares = new ArrayList<>();
        int failedPeers = 0;

        for (Acceptor<T> acceptor : acceptors) {
            Prepare<T> prepare = null;
            try {
                prepare = acceptor.prepare(n);
            } catch (Exception e) {
                failedPeers++;
                //TODO
                e.printStackTrace();
                continue;
            }
            if (failedPeers >= getMajority()) {
                //TODO
                throw new IllegalStateException();
            }
            checkPrepare(acceptor, prepare);
            prepares.add(prepare);
        }

        int ok = 0;
        T max = null;
        for (Prepare<T> prepare : prepares) {
            if (!prepare.isOk()) {
                continue;
            }
            ok++;
            T na = prepare.getNa();
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

    private void checkPrepare(Acceptor<T> acceptor, Prepare<T> prepare) {
        if (prepare.isOk()) {
            Preconditions.checkState(Objects.equals(prepare.getN(), n),
                    acceptor + " prepare请求返回序号为" + prepare.getN() + "，应该是" + n);
        }
    }

    boolean accept() {
        int ok = 0;
        int failedPeers = 0;
        Preconditions.checkNotNull(agreement);

        for (Acceptor<T> acceptor : acceptors) {
            Accept<T> accept = null;
            try {
                accept = acceptor.accept(n, agreement);
            } catch (Exception e) {
                //TODO
                failedPeers++;
                e.printStackTrace();
                continue;
            }

            if (failedPeers >= getMajority()) {
                //TODO
                throw new IllegalStateException();
            }

            checkAccept(acceptor, accept);
            if (accept.isOk()) {
                ok++;
            }
        }
        return ok >= majority;
    }

    private void checkAccept(Acceptor<T> acceptor, Accept<T> accept) {
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

    void setN(T n) {
        this.n = n;
    }

    byte[] getAgreement() {
        return this.agreement;
    }

}