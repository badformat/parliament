package io.github.parliament.rsm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.MapMaker;
import io.github.parliament.paxos.Paxos;
import io.github.parliament.paxos.Proposal;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.acceptor.ExceptionDeferAcceptor;
import io.github.parliament.paxos.proposer.TimestampSequence;
import io.github.parliament.server.InetLearner;
import io.github.parliament.server.PaxosSyncServer;
import io.github.parliament.server.RemoteAcceptorSyncProxy;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

/**
 *
 * @author zy
 */
public class PaxosReplicateStateMachine extends Paxos<String> implements AcceptorFactory<String> {
    @Getter
    private          ProposalPersistenceService                                proposalPersistenceService;
    @Getter
    private volatile InetSocketAddress                                         me;
    private volatile List<InetSocketAddress>                                   others;
    private volatile ConcurrentMap<InetSocketAddress, RemoteAcceptorSyncProxy> remoteAcceptorProxies = new MapMaker()
            .makeMap();

    private volatile LoadingCache<Integer, RsmLocalAcceptor> acceptorsCache;
    @Getter(AccessLevel.PACKAGE)
    private          InetLearner                             learner;

    private          PaxosSyncServer server;
    private volatile int             threadNo;
    private volatile boolean         started = false;

    @Builder
    public PaxosReplicateStateMachine(List<InetSocketAddress> peers,
                                      InetSocketAddress me,
                                      int threadNo,
                                      ProposalPersistenceService proposalPersistenceService
    ) {
        others = new ArrayList<>(peers);
        others.remove(me);
        this.me = me;
        this.threadNo = threadNo;
        this.proposalPersistenceService = proposalPersistenceService;
        this.sequence = new TimestampSequence();
        this.acceptorsCache = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<Integer, RsmLocalAcceptor>() {
                    public RsmLocalAcceptor load(Integer round) throws Exception {
                        Optional<Proposal> proposal = proposalPersistenceService.getProposal(round);
                        RsmLocalAcceptor acceptor = RsmLocalAcceptor.builder().round(round).proposalService(
                                proposalPersistenceService).build();
                        if (proposal.isPresent()) {
                            String n = sequence.next();
                            acceptor.prepare(n);
                            acceptor.accept(n, proposal.get().getAgreement());
                        }
                        return acceptor;
                    }
                });
        this.server = PaxosSyncServer.builder()
                .acceptorFactory(this)
                .peersNo(peers.size())
                .proposalService(proposalPersistenceService)
                .me(me)
                .build();
        this.learner = InetLearner.builder().others(others).proposalService(proposalPersistenceService).build();
    }

    public void start() throws Exception {
        synchronized (this) {
            if (started) {
                return;
            }
            executorService = Executors.newFixedThreadPool(threadNo);
            server.start();
            started = true;
        }
        this.pull();
    }

    public void shutdown() throws IOException {
        synchronized (this) {
            if (!started) {
                return;
            }
            server.shutdown();
            executorService.shutdown();
            started = false;
        }
    }

    Callable<Boolean> pull() throws Exception {
        int begin = maxRound();
        return () -> learner.pullAll(begin);
    }

    public Future<Boolean> pull(int round) {
        return executorService.submit(() -> learner.pull(round));
    }

    public int nextRound() throws Exception {
        return proposalPersistenceService.nextRound();
    }

    public int minRound() throws Exception {
        return proposalPersistenceService.minRound();
    }

    public void forget(int round) throws Exception {
        int max = Math.min(round, proposalPersistenceService.maxRound());
        synchronized (this) {
            int safe = learner.learnMax().stream().reduce(Integer.MAX_VALUE, (a, b) -> a > b ? b : a);
            proposalPersistenceService.forget(Math.min(max, safe));
        }
    }

    public int maxRound() throws Exception {
        return proposalPersistenceService.maxRound();
    }

    public Optional<Proposal> proposal(int round) throws Exception {
        return proposalPersistenceService.getProposal(round);
    }

    synchronized protected Collection<Acceptor<String>> getAcceptors(int round) {
        for (InetSocketAddress peer : others) {
            remoteAcceptorProxies.computeIfAbsent(peer, (k) -> new RemoteAcceptorSyncProxy(peer));
        }

        List<Acceptor<String>> acceptors = remoteAcceptorProxies.values().stream().
                map(a -> a.createAcceptorForRound(round)).collect(Collectors.toList());

        acceptors.add(createLocalAcceptorFor(round));
        Preconditions.checkState(acceptors.size() == others.size() + 1);
        return acceptors;
    }

    @Override
    public Acceptor<String> createLocalAcceptorFor(int round) {
        try {
            return acceptorsCache.get(round);
        } catch (ExecutionException e) {
            return ExceptionDeferAcceptor.<String>builder().exception(e).build();
        }
    }
}