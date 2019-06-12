package io.github.parliament;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ReplicateInputMachineTest {
    private ReplicateStateMachine rsm;
    private StateTransfer processor = mock(StateTransfer.class);
    private Persistence persistence = new MockPersistence();
    private MockPaxos coordinator = new MockPaxos();
    private Sequence<Integer> sequence = new IntegerSequence();

    @BeforeEach
    void setUp() throws Exception {
        rsm = ReplicateStateMachine
                .builder()
                .persistence(persistence)
                .coordinator(coordinator)
                .sequence(sequence)
                .build();
        rsm.setStateTransfer(processor);
        Output output = mock(Output.class);
        when(output.getContent()).thenReturn("output".getBytes());
        when(processor.transform(any())).thenReturn(output);
    }

    @AfterEach
    void tearDown() {
        sequence.set(0);
        coordinator.clear();
    }

    @Test
    void submit() throws IOException,
            ExecutionException,
            InterruptedException {
        Input submitted = rsm.newState("content".getBytes());
        CompletableFuture<Output> future = rsm.submit(submitted);
        rsm.apply();
        Output output = future.get();
        when(output.getUuid()).thenReturn(submitted.getUuid());
        assertEquals(submitted.getId(), output.getId());
        assertArrayEquals(submitted.getUuid(), output.getUuid());
        assertArrayEquals("output".getBytes(), output.getContent());
    }

    @Test
    void follow() throws Exception {
        CompletableFuture<Output> f1 = rsm.submit(rsm.newState("state1".getBytes()));
        CompletableFuture<Output> f2 = rsm.submit(rsm.newState("state2".getBytes()));

        assertThrows(TimeoutException.class, () -> f1.get(1, TimeUnit.SECONDS));

        rsm.apply();
        assertArrayEquals("output".getBytes(), f1.get(1, TimeUnit.SECONDS).getContent());
        rsm.apply();
        assertArrayEquals("output".getBytes(), f2.get(1, TimeUnit.SECONDS).getContent());
        verify(processor, times(2)).transform(any());
    }

    @Test
    void followInThread() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        rsm.start(processor, executor);
        int i = 0;
        Stream.iterate(i, (n) -> n + 1).limit(100).parallel()
                .map((n) -> {
                    try {
                        rsm.submit(rsm.newState((n + "").getBytes()));
                    } catch (IOException | ExecutionException e) {
                        fail(e);
                    }
                    return n;
                }).toArray();

        while (rsm.done() < 99) {
            Thread.sleep(2);
        }
        assertEquals(99, rsm.max());
        assertEquals(99, rsm.done());
        verify(processor, times(100)).transform(any());
        executor.shutdown();
    }

    @Test
    void max() throws ExecutionException, IOException {
        rsm.submit(rsm.newState("state1".getBytes()));
        rsm.apply();
        assertEquals(0, rsm.max());
    }

    @Test
    void done() throws IOException, ExecutionException {
        rsm.submit(rsm.newState("newState".getBytes()));
        rsm.apply();

        rsm.submit(rsm.newState("newState".getBytes()));
        rsm.apply();

        rsm.submit(rsm.newState("newState".getBytes()));
        rsm.apply();

        assertEquals(2, rsm.done());
    }

    @Test
    void forget() throws IOException, ExecutionException {
        assertThrows(IllegalStateException.class, () -> rsm.forget(0));
        rsm.submit(rsm.newState("newState".getBytes()));
        rsm.apply();
        assertEquals(0, rsm.done());
        rsm.forget(rsm.done());
    }

}