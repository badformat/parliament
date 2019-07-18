package io.github.parliament;

import io.github.parliament.page.Pager;
import io.github.parliament.skiplist.SkipList;
import lombok.NonNull;
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
        Pager.init(tempDir, 64 * 1024, 16 * 1024);
        Pager pager = Pager.builder().path(tempDir).build();

        SkipList.init(tempDir, 6, pager);
        SkipList skipList = SkipList.builder().path(tempDir).pager(pager).build();
        Sequence<Integer> sequence = new IntegerSequence();
        MockPaxos coordinator = new MockPaxos();
        ReplicateStateMachine ret = ReplicateStateMachine
                .builder()
                .persistence(skipList)
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