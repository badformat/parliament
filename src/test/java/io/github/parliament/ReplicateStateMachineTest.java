package io.github.parliament;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ReplicateStateMachineTest {
    private ReplicateStateMachine rsm;
    private EventProcessor processor = mock(EventProcessor.class);
    private Persistence persistence = new MockPersistence();
    private MockCoordinator coordinator = new MockCoordinator();
    private Sequence<Integer> sequence = new IntegerSequence();

    @BeforeEach
    void setUp() {
        rsm = ReplicateStateMachine
                .builder()
                .persistence(persistence)
                .coordinator(coordinator)
                .sequence(sequence)
                .build();
        rsm.setEventProcessor(processor);
    }

    @AfterEach
    void tearDown() {
        sequence.set(0);
        coordinator.clear();
    }

    @Test
    void submit() throws IOException,
            ExecutionException,
            InterruptedException,
            ClassNotFoundException {
        State submitted = rsm.state("content".getBytes());
        CompletableFuture<State> future = rsm.submit(submitted);
        rsm.follow();
        State state = future.get();
        assertEquals(submitted.getId(), state.getId());
        assertArrayEquals(submitted.getUuid(), state.getUuid());
        assertArrayEquals(submitted.getContent(), state.getContent());
    }

    @Test
    void follow() throws IOException,
            ExecutionException,
            InterruptedException,
            ClassNotFoundException,
            TimeoutException {
        CompletableFuture<State> f1 = rsm.submit(rsm.state("state1".getBytes()));
        CompletableFuture<State> f2 = rsm.submit(rsm.state("state2".getBytes()));

        assertThrows(TimeoutException.class, () -> f1.get(1, TimeUnit.SECONDS));

        rsm.follow();
        assertTrue(f1.get(1, TimeUnit.SECONDS).isProcessed());
        rsm.follow();
        assertTrue(f2.get(1, TimeUnit.SECONDS).isProcessed());
        verify(processor, times(2)).process(any());
    }

    @Test
    void followInThread() throws InterruptedException {
        Thread t = new Thread(rsm);
        t.start();
        int i = 0;
        Stream.iterate(i, (n) -> n + 1).limit(100).parallel()
                .map((n) -> {
                    try {
                        rsm.submit(rsm.state((n + "").getBytes()));
                    } catch (IOException e) {
                        fail(e);
                    }
                    return n;
                }).toArray();

        while (rsm.done() < 99) {
            Thread.sleep(2);
        }
        assertEquals(99, rsm.max());
        assertEquals(99, rsm.done());
        verify(processor, times(100)).process(any());
        t.interrupt();
    }

    @Test
    void max() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        rsm.submit(rsm.state("state1".getBytes()));
        rsm.follow();
        assertEquals(0, rsm.max());
    }

    @Test
    void done() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException {
        rsm.submit(rsm.state("state".getBytes()));
        rsm.follow();

        rsm.submit(rsm.state("state".getBytes()));
        rsm.follow();

        rsm.submit(rsm.state("state".getBytes()));
        rsm.follow();

        assertEquals(2, rsm.done());
    }

    @Test
    void forget() throws IOException, InterruptedException, ExecutionException, ClassNotFoundException {
        assertThrows(IllegalStateException.class, () -> rsm.forget(0));
        rsm.submit(rsm.state("state".getBytes()));
        rsm.follow();
        assertEquals(0, rsm.done());
        rsm.forget(rsm.done());
    }

}