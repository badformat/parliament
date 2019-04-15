package io.github.parliament.rsm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Acceptor;
import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.acceptor.Prepare;
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
    static private final RejectAcceptor                                            rejectAcceptor        = new RejectAcceptor();
    @Getter
    private              ProposalPersistenceService                                proposalPersistenceService;
    @Getter
    private volatile     InetSocketAddress                                         me;
    private volatile     List<InetSocketAddress>                                   others;
    private volatile     ConcurrentMap<InetSocketAddress, RemoteAcceptorSyncProxy> remoteAcceptorProxies = new MapMaker()
            .makeMap();
    private volatile     boolean                                                   joined                = true;

    private volatile LoadingCache<Integer, RoundLocalAcceptor> acceptors;
    @Getter(AccessLevel.PACKAGE)
    private          InetLearner                               learner;

    private static class RejectAcceptor implements Acceptor<String> {

        @Override
        public Prepare<String> prepare(String n) {
            return Prepare.reject(n);
        }

        @Override
        public Accept<String> accept(String n, byte[] value) {
            return Accept.reject(n);
        }

        @Override
        public void decide(byte[] agreement) {
            throw new IllegalStateException();
        }
    }

    private PaxosSyncServer server;

    @Builder
    public PaxosReplicateStateMachine(List<InetSocketAddress> peers,
                                      InetSocketAddress me,
                                      ExecutorService executorService,
                                      ProposalPersistenceService proposalPersistenceService
    ) {
        others = new ArrayList<>(peers);
        others.remove(me);
        this.me = me;
        this.executorService = executorService;
        this.proposalPersistenceService = proposalPersistenceService;
        this.sequence = new TimestampSequence();
        this.acceptors = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<Integer, RoundLocalAcceptor>() {
                    public RoundLocalAcceptor load(Integer round) throws Exception {
                        Optional<Proposal> proposal = proposalPersistenceService.getProposal(round);
                        RoundLocalAcceptor acceptor = RoundLocalAcceptor.builder().round(round).proposalService(
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
                .executorService(executorService)
                .proposalService(proposalPersistenceService)
                .me(me)
                .build();

        this.learner = InetLearner.builder().others(others).proposalService(proposalPersistenceService).build();
    }

    public void start() throws Exception {
        this.server.start();
        //TODO this.pull();
    }

    public void shutdown() throws Exception {
        super.shutdown();
        this.server.shutdown();
    }

    Future<Boolean> pull() throws Exception {
        int begin = maxRound();
        return executorService.submit(() -> learner.pullAll(begin));
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

    @Override
    synchronized protected Collection<Acceptor<String>> getAcceptors(int round) {
        List<Acceptor<String>> acceptors = new ArrayList<>();
        for (InetSocketAddress peer : others) {
            RemoteAcceptorSyncProxy acceptor = remoteAcceptorProxies.get(peer);

            if (acceptor != null) {
                if (!acceptor.getRemote().isOpen()) {
                    remoteAcceptorProxies.remove(peer);
                }
            }
            if (!remoteAcceptorProxies.containsKey(peer)) {
                try {
                    SocketChannel remote = SocketChannel.open(peer);
                    remoteAcceptorProxies.computeIfAbsent(peer, (k) -> new RemoteAcceptorSyncProxy(remote));
                } catch (IOException e) {
                    // TODO log
                    //e.printStackTrace();
                    acceptors.add(rejectAcceptor);
                }
            }
        }

        acceptors.addAll(remoteAcceptorProxies.values().stream().
                map(r -> new Acceptor<String>() {
                    @Override
                    public Prepare<String> prepare(String n) throws Exception {
                        return r.delegatePrepare(round, n);
                    }

                    @Override
                    public Accept<String> accept(String n, byte[] value) throws Exception {
                        Preconditions.checkNotNull(value);
                        return r.delegateAccept(round, n, value);
                    }

                    @Override
                    public void decide(byte[] agreement) throws Exception {
                        Preconditions.checkNotNull(agreement);
                        r.delegateDecide(round, agreement);
                    }
                }).collect(Collectors.toList()));

        acceptors.add(createLocalAcceptorFor(round));
        Preconditions.checkState(acceptors.size() == others.size() + 1);
        return acceptors;
    }

    void joinAgreementProcess() {
        this.joined = true;
    }

    void leaveAgreementProcess() {
        this.joined = false;
    }

    @Override
    public Acceptor<String> createLocalAcceptorFor(int round) {
        if (!joined) {
            return rejectAcceptor;
        }

        try {
            return acceptors.get(round);
        } catch (ExecutionException e) {
            //TODO e.printStackTrace();
            return rejectAcceptor;
        }
    }
}