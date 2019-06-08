package io.github.parliament;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ReplicateStateMachineWithPersistenceTest {
    private static ReplicateStateMachine rsm;
    private static EventProcessor processor = mock(EventProcessor.class);
    private static ExecutorService executor = Executors.newFixedThreadPool(10);

    @BeforeAll
    static void beforeAll() throws IOException {
        rsm = create("./rsm/1");
        rsm.start(processor, executor);
    }

    static private ReplicateStateMachine create(String path) throws IOException {
        PagePersistence persistence = PagePersistence.builder().path(Paths.get(path)).build();
        Sequence<Integer> sequence = new IntegerSequence();
        MockPaxos coordinator = new MockPaxos();
        ReplicateStateMachine ret = ReplicateStateMachine
                .builder()
                .persistence(persistence)
                .coordinator(coordinator)
                .sequence(sequence)
                .build();
        ret.setEventProcessor(processor);
        return ret;
    }

    @AfterAll
    static void afterAll() throws IOException {
        executor.shutdown();
        Files.walk(Paths.get("./rsm"))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void submit() {
        Stream.iterate(0, (i) -> i + 1).limit(10).parallel()
                .map((i) -> {
                    try {
                        byte[] c = (i + "content").getBytes();
                        State s = rsm.state(c);
                        assertTrue(rsm.done() < s.getId(), "done:" + rsm.done() + ",id:" + s.getId());
                        State r = rsm.submit(s).get(10, TimeUnit.SECONDS);
                        assertArrayEquals(c, s.getContent());
                        assertTrue(rsm.done() >= r.getId() - 1, "done:" + rsm.done() + ",id:" + s.getId());
                    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                        fail(e);
                    }
                    return i;
                }).toArray();
    }

    @Test
    void redoLog() throws IOException {
        ReplicateStateMachine rsm1 = create("./rsm/2");
        rsm1.getPersistence().put(ReplicateStateMachine.RSM_DONE_REDO,
                ByteBuffer.allocate(4).putInt(88).array());
        rsm1.start(processor, executor);
        assertEquals(88, rsm1.done());
        assertEquals(Integer.valueOf(89), rsm1.state("c".getBytes()).getId());
    }
}