package io.github.parliament.kv;

import io.github.parliament.*;
import io.github.parliament.resp.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
    void putAndGet() throws IOException {
        Input input = requestState("put", "key", "value");
        keyValueEngine.transform(input);
        input = requestState("get", "key");
        Output o = keyValueEngine.transform(input);
        RespBulkString s = RespDecoder.create().decode(o.getContent()).get();

        assertEquals("value", new String(s.getContent()));
    }

    @Test
    void concurrent() {
        int i = 0;
        Stream.iterate(i, (k) -> k + 1).limit(200).parallel()
                .map((n) -> {
                    try {
                        Input input = requestState("put", "key" + n, "value" + n);
                        Output o = keyValueEngine.transform(input);
                        assertArrayEquals(RespInteger.with(1).toBytes(), o.getContent());
                        input = requestState("get", "key" + n);
                        o = keyValueEngine.transform(input);
                        RespBulkString s = RespDecoder.create().decode(o.getContent()).get();
                        assertArrayEquals(("value" + n).getBytes(), s.getContent(), "fail at " + n);
                    } catch (Exception e) {
                        fail("fail at " + n, e);
                    }
                    return n * 2;
                }).toArray();
    }

    @Test
    void execute() throws Exception {
        Input original = mock(Input.class);
        when(original.getId()).thenReturn(1);
        when(original.getUuid()).thenReturn("uuid".getBytes());
        when(rsm.newState(any())).thenReturn(original);

        Output output = mock(Output.class);
        when(rsm.submit(any())).thenReturn(CompletableFuture.completedFuture(output));
        when(output.getUuid()).thenReturn("uuid".getBytes());
        when(output.getContent()).thenReturn(RespInteger.with(2).toBytes());

        assertEquals(RespInteger.with(2),
                keyValueEngine.submit1(request("get", "key")).get());
    }

    @Test
    void conflict() throws Exception {
        Input original = mock(Input.class);
        when(original.getId()).thenReturn(1);
        when(original.getUuid()).thenReturn("tag1".getBytes());
        when(rsm.newState(any())).thenReturn(original);

        Output output = mock(Output.class);
        when(rsm.submit(any())).thenReturn(CompletableFuture.completedFuture(output));

        assertEquals(RespError.withUTF8("共识冲突"),
                keyValueEngine.submit1(request("get", "key")).get());
    }

    private Input requestState(String cmd, String... args) {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        byte[] content = RespArray.with(datas).toBytes();

        return Input.builder().content(content).id(1).uuid("uuid".getBytes()).build();
    }

    private byte[] request(String cmd, String... args) {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        return RespArray.with(datas).toBytes();
    }
}