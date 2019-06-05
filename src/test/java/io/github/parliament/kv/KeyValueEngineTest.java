package io.github.parliament.kv;

import io.github.parliament.Persistence;
import io.github.parliament.MockPersistence;
import io.github.parliament.ReplicateStateMachine;
import io.github.parliament.State;
import io.github.parliament.resp.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author zy
 */
class KeyValueEngineTest {
    private KeyValueEngine keyValueEngine;
    private Path path = Paths.get("./test");
    private ExecutorService executorService = mock(ExecutorService.class);
    private Persistence persistence;
    private ReplicateStateMachine rsm = mock(ReplicateStateMachine.class);

    @BeforeEach
    void beforeEach() throws IOException {

        persistence = new MockPersistence();
        keyValueEngine = KeyValueEngine.builder()
                .executorService(executorService)
                .persistence(persistence)
                .rsm(rsm)
                .build();

        keyValueEngine.start();
    }

    @Test
    void putAndGet() throws Exception {
        keyValueEngine.process(request("put", "key", "value"));
        byte[] value = keyValueEngine.process(request("get", "key"));
        RespBulkString s = RespDecoder.create().decode(value).get();

        assertEquals("value", new String(s.getContent()));
    }

    @Test
    void concurrent() {
        int i = 0;
        Stream.iterate(i, (k) -> k + 1).limit(200).parallel()
                .map((n) -> {
                    try {
                        byte[] output = keyValueEngine
                                .process(request("put", "key" + n, "value" + n));
                        assertArrayEquals(RespInteger.with(1).toBytes(), output);

                        output = keyValueEngine
                                .process(request("get", "key" + n));
                        RespBulkString s = RespDecoder.create().decode(output).get();
                        assertArrayEquals(("value" + n).getBytes(), s.getContent(), "fail at " + n);
                    } catch (Exception e) {
                        fail("fail at " + n, e);
                    }
                    return n * 2;
                }).toArray();
    }

    @Test
    void execute() throws Exception {
        State original = mock(State.class);
        when(original.getId()).thenReturn(1);
        when(original.getUuid()).thenReturn("uuid".getBytes());
        when(rsm.state(any())).thenReturn(original);

        State consensus = mock(State.class);
        when(rsm.submit(any())).thenReturn(CompletableFuture.completedFuture(consensus));
        when(consensus.getUuid()).thenReturn("uuid".getBytes());
        when(consensus.getOutput()).thenReturn(RespInteger.with(2).toBytes());

        assertEquals(RespInteger.with(2),
                keyValueEngine.execute1(request("get", "key")).get());
    }

    @Test
    void conflict() throws Exception {
        State original = mock(State.class);
        when(original.getId()).thenReturn(1);
        when(original.getUuid()).thenReturn("tag1".getBytes());
        when(rsm.state(any())).thenReturn(original);

        State consensus = mock(State.class);
        when(rsm.submit(any())).thenReturn(CompletableFuture.completedFuture(consensus));

        assertEquals(RespError.withUTF8("共识冲突"),
                keyValueEngine.execute1(request("get", "key")).get());
    }

    private byte[] request(String cmd, String... args) {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        return RespArray.with(datas).toBytes();
    }
}