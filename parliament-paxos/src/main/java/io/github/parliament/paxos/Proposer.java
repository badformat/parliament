package io.github.parliament.paxos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author zy
 * @version $Id: Proposer.java, v 0.1 2019年03月08日 3:17 PM zy Exp $
 */
public class Proposer<T extends Comparable<T>> {
    private Collection<Acceptor<T>> acceptors;
    private int majority = Integer.MAX_VALUE;
    private ProposalSeqNoGenerator<T> seqNoGenerator;
    private boolean decided = false;
    private T n;
    private T maxN;
    byte[] va = null;

    public Proposer(Collection<Acceptor<T>> acceptors, ProposalSeqNoGenerator<T> seqNoGenerator) {
        this.acceptors = acceptors;
        this.majority = acceptors.size() / 2;
        this.seqNoGenerator = seqNoGenerator;
    }

    public byte[] propose(byte[] proposal) {
        while (!decided) {
            n = seqNoGenerator.next();
            if (prepare(proposal)) {
                decided = accept();
            }
        }
        //TODO send decided

        return va;
    }

    private boolean prepare(byte[] proposal) {
        va = proposal;
        List<Prepare<T>> prepares = new ArrayList<>();

        for (Acceptor<T> acceptor : acceptors) {
            prepares.add(acceptor.prepare(n));
        }

        int ok = 0;
        for (Prepare<T> prepare : prepares) {
            if (prepare.isOk()) {
                ok++;
            }
            if (prepare.getNa().compareTo(maxN) > 0) {
                maxN = prepare.getN();
                if (prepare.getVa() != null && prepare.getVa().length > 0) {
                    va = prepare.getVa();
                }
            }
        }

        return ok > majority;

    }

    private boolean accept() {
        List<Accept<T>> accepts = new ArrayList<>();
        int ok = 0;

        for (Acceptor<T> acceptor : acceptors) {
            Accept<T> accept = acceptor.accept(n, va);
            accepts.add(accept);
        }

        for (Accept<T> accept : accepts) {
            if (accept.isOk() && accept.getN().compareTo(n) == 0) {
                ok++;
            }
        }

        return ok > majority;
    }
}