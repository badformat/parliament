package io.github.parliament.rsm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import io.github.parliament.paxos.learner.Learner;
import io.github.parliament.paxos.proposer.TimestampSequence;
import io.github.parliament.rsm.StateMachineEvent.Status;
import io.github.parliament.network.PaxosSyncServer;
import io.github.parliament.network.RemoteAcceptorSyncProxy;
import lombok.Builder;
import lombok.Getter;

/**
 *
 * @author zy
 */
public class PaxosReplicateStateMachine extends Paxos<String> implements AcceptorFactory<String> {
    static private final RejectAcceptor                                            rejectAcceptor        = new RejectAcceptor();
    @Getter
    private              RoundPersistenceService                                   roundPersistenceService;
    @Getter
    private volatile     InetSocketAddress                                         me;
    private volatile     List<InetSocketAddress>                                   peers;
    private volatile     ConcurrentMap<InetSocketAddress, RemoteAcceptorSyncProxy> remoteAcceptorProxies = new MapMaker()
            .makeMap();
    private volatile     boolean                                                   joined                = true;

    private volatile LoadingCache<Integer, RoundLocalAcceptor> acceptors;
    private          Learner                                   learner;

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
                                      RoundPersistenceService roundPersistenceService
    ) {
        this.peers = peers;
        this.me = me;
        this.executorService = executorService;
        this.roundPersistenceService = roundPersistenceService;
        this.sequence = new TimestampSequence();
        this.acceptors = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Integer, RoundLocalAcceptor>() {
                            public RoundLocalAcceptor load(Integer round) throws Exception {
                                Optional<RoundLocalAcceptor> acc = roundPersistenceService.getAcceptorFor(round);
                                return acc.orElseGet(() -> RoundLocalAcceptor
                                        .builder()
                                        .round(round)
                                        .reachedAgreement(roundPersistenceService)
                                        .build());
                            }
                        });
        this.server = PaxosSyncServer.builder()
                .acceptorFactory(this)
                .executorService(executorService)
                .me(me)
                .build();
    }

    public void start() throws Exception {
        this.server.start();
        this.sync();
    }

    void sync() throws Exception {
        int max = learner.max();
        int localMax = maxRound();
        if (localMax < max) {
            Stream<Proposal> rounds = learner.learn(localMax + 1, max);
            rounds.forEach(p -> {
                //TODO RoundLocalAcceptor acceptor = new RoundLocalAcceptor();
                //TODO this.roundPersistenceService.saveAcceptor(acceptor);
            });
        }
    }

    public void shutdown() throws Exception {
        super.shutdown();
        this.server.shutdown();
    }

    public int nextRound() throws Exception {
        return roundPersistenceService.nextRound();
    }

    public int minRound() throws Exception {
        return roundPersistenceService.minRound();
    }

    public synchronized void forgetRoundsTo(int no) throws Exception {
        roundPersistenceService.forgetRoundsTo(no);
        roundPersistenceService.setMin(no);
    }

    public int maxRound() throws Exception {
        return roundPersistenceService.maxRound();
    }

    public StateMachineEvent event(int round) throws Exception {
        if (round < roundPersistenceService.minRound()) {
            return StateMachineEvent.deleted;
        }

        Optional<RoundLocalAcceptor> acceptor = roundPersistenceService.getAcceptorFor(round);
        if (!acceptor.isPresent()) {
            return StateMachineEvent.unknown;
        }
        return StateMachineEvent.builder().agreement(acceptor.get().getVa()).round(round).status(Status.decided).build();
    }

    @Override
    synchronized protected Collection<Acceptor<String>> getAcceptors(int round) {
        List<Acceptor<String>> acceptors = new ArrayList<>();
        for (InetSocketAddress peer : peers) {
            if (Objects.equals(peer, me)) {
                continue;
            }
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
        Preconditions.checkState(acceptors.size() == peers.size());
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