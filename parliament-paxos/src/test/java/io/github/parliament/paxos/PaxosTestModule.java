package io.github.parliament.paxos;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

import io.github.parliament.paxos.acceptor.AcceptorFactory;
import io.github.parliament.paxos.proposer.ProposerFactory;
import io.github.parliament.paxos.proposer.Sequence;
import io.github.parliament.paxos.proposer.TimestampSequence;

public class PaxosTestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(new TypeLiteral<ProposerFactory<?>>() {
        }).to(ProposerTestFactory.class);
        
        bind(new TypeLiteral<AcceptorFactory<String>>() {
        }).to(AcceptorTestFactory.class);
        
        bind(ExecutorService.class).toInstance(Executors.newFixedThreadPool(15));
        
        bind(new TypeLiteral<Sequence<String>>() {
        }).to(TimestampSequence.class);
    }
}
