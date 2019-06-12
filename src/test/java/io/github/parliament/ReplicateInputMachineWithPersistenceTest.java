package io.github.parliament;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReplicateInputMachineWithPersistenceTest {
    private static ReplicateStateMachine rsm;
    private static StateTransfer processor = mock(StateTransfer.class);
    private static ExecutorService executor = Executors.newFixedThreadPool(10);
    private static Path tempDir;

    @BeforeAll
    static void beforeAll() throws Exception {
        tempDir = Files.createTempDirectory("test");
        rsm = create("1");
        rsm.start(processor, executor);
    }

    static private ReplicateStateMachine create(String dir) throws Exception {
        PagePersistence persistence = PagePersistence.builder().path(tempDir.resolve(dir)).build();
        Sequence<Integer> sequence = new IntegerSequence();
        MockPaxos coordinator = new MockPaxos();
        ReplicateStateMachine ret = ReplicateStateMachine
                .builder()
                .persistence(persistence)
                .coordinator(coordinator)
                .sequence(sequence)
                .build();
        ret.setStateTransfer(processor);
        when(processor.transform(any())).thenReturn(mock(Output.class));
        return ret;
    }

    @AfterAll
    static void afterAll() throws IOException {
        executor.shutdown();
        Files.walk(tempDir)
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
                        Input s = rsm.newState(c);
                        assertTrue(rsm.done() < s.getId(), "done:" + rsm.done() + ",id:" + s.getId());
                        Output o = rsm.submit(s).get(10, TimeUnit.SECONDS);
                        assertArrayEquals(c, s.getContent());
                        assertTrue(rsm.done() >= o.getId() - 1, "done:" + rsm.done() + ",id:" + s.getId());
                    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                        fail(e);
                    }
                    return i;
                }).toArray();
    }

    @Test
    void redoLog() throws Exception {
        ReplicateStateMachine rsm1 = create("2");
        rsm1.getPersistence().put(ReplicateStateMachine.RSM_DONE_REDO,
                ByteBuffer.allocate(4).putInt(88).array());
        rsm1.start(processor, executor);
        assertEquals(88, rsm1.done());
        assertEquals(Integer.valueOf(89), rsm1.newState("c".getBytes()).getId());
    }
}