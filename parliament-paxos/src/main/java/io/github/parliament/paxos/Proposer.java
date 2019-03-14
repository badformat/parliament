package io.github.parliament.paxos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Observable;

import com.google.common.base.Preconditions;

/**
 *
 * @author zy
 * @version $Id: Proposer.java, v 0.1 2019年03月08日 3:17 PM zy Exp $
 */
public class Proposer<T extends Comparable<T>> extends Observable {
    private Collection<Acceptor<T>> acceptors;
    private int majority = Integer.MAX_VALUE;
    private Sequence<T> sequence;
    private boolean decided = false;
    private T n;
    private T max;
    private byte[] agreement;
    Proposal proposal = null;

    public Proposer(Collection<Acceptor<T>> acceptors, Sequence<T> sequence, Proposal proposal) {
        this.acceptors = acceptors;
        this.majority = calcMajority(acceptors.size());
        this.sequence = sequence;
        this.proposal = proposal;
        this.agreement = proposal.getContent();
    }

    public void propose() {
        while (!decided) {
            n = sequence.next();
            if (prepare()) {
                decided = accept();
            }
        }

        for (Acceptor<T> acceptor : acceptors) {
            acceptor.decided(agreement);
        }
    }

    public boolean isDecided() {
        return decided;
    }

    boolean prepare() {
        List<Prepare<T>> prepares = new ArrayList<>();

        for (Acceptor<T> acceptor : acceptors) {
            Prepare<T> prepare = acceptor.prepare(n);
            checkPrepare(acceptor, prepare);
            prepares.add(prepare);
        }

        int ok = 0;
        max = n;
        for (Prepare<T> prepare : prepares) {
            if (!prepare.isOk()) {
                continue;
            }
            ok++;
            T na = prepare.getNa();
            if (na.compareTo(max) > 0) {
                max = na;
                agreement = prepare.getVa();
            }
        }

        return ok >= majority;
    }

    private void checkPrepare(Acceptor<T> acceptor, Prepare<T> prepare) {
        if (prepare.isOk()) {
            Preconditions.checkState(Objects.equals(prepare.getN(), n),
                    acceptor + " prepare请求返回序号为" + prepare.getN() + "，应该是" + n);
            Preconditions.checkNotNull(prepare.getNa(), acceptor + "返回prepare的当前接收编号为null");
            Preconditions.checkNotNull(prepare.getVa(), acceptor + "返回prepare的当前接收值为null");
            Preconditions.checkState(prepare.getVa().length > 0, acceptor + "返回prepare的当前接收值长度为0");
        }
    }

    boolean accept() {
        int ok = 0;
        for (Acceptor<T> acceptor : acceptors) {
            Accept<T> accept = acceptor.accept(n, agreement);
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