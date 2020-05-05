package io.github.parliament.kv;

import io.github.parliament.*;
import io.github.parliament.page.Pager;
import io.github.parliament.resp.*;
import io.github.parliament.skiplist.SkipList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
    void beforeEach() throws IOException, ExecutionException {
        Path path = Paths.get("./testdb");
        Pager.init(path, 512, 64);
        Pager pager = Pager.builder().path(path).build();

        SkipList.init(path, 6, pager);
        SkipList skipList = SkipList.builder().path(path).pager(pager).build();
        keyValueEngine = KeyValueEngine.builder()
                .executorService(executorService)
                .skipList(skipList)
                .rsm(rsm)
                .build();

        keyValueEngine.start();
    }

    @AfterEach
    void afterEach() throws IOException {
        Files.walk(Paths.get("./testdb")).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    @Test
    void setAndGet() throws IOException, ExecutionException {
        ReplicateStateMachine.Input input = requestState("set", "key", "value");
        keyValueEngine.transform(input);
        input = requestState("get", "key");
        ReplicateStateMachine.Output o = keyValueEngine.transform(input);
        RespBulkString s = RespDecoder.create().decode(o.getContent()).get();

        assertEquals("value", new String(s.getContent()));
    }

    @Test
    void concurrent() {
        int i = 0;
        Stream.iterate(i, (k) -> k + 1).limit(200).parallel()
                .map((n) -> {
                    try {
                        ReplicateStateMachine.Input input = requestState("set", "key" + n, "value" + n);
                        ReplicateStateMachine.Output o = keyValueEngine.transform(input);
                        assertArrayEquals(RespSimpleString.withUTF8("OK").toBytes(), o.getContent());
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
        ReplicateStateMachine.Input original = mock(ReplicateStateMachine.Input.class);
        when(original.getId()).thenReturn(1);
        when(original.getUuid()).thenReturn("uuid".getBytes());
        when(rsm.newState(any())).thenReturn(original);

        ReplicateStateMachine.Output output = mock(ReplicateStateMachine.Output.class);
        when(rsm.submit(any())).thenReturn(CompletableFuture.completedFuture(output));
        when(output.getUuid()).thenReturn("uuid".getBytes());
        when(output.getContent()).thenReturn(RespInteger.with(2).toBytes());

        ByteBuffer byteBuf = keyValueEngine.execute(request("get", "key"), 3, TimeUnit.SECONDS);
        assertEquals(RespInteger.with(2).toByteBuffer(), byteBuf);
    }

    @Test
    void conflict() throws Exception {
        ReplicateStateMachine.Input original = mock(ReplicateStateMachine.Input.class);
        when(original.getId()).thenReturn(1);
        when(original.getUuid()).thenReturn("tag1".getBytes());
        when(rsm.newState(any())).thenReturn(original);

        ReplicateStateMachine.Output output = mock(ReplicateStateMachine.Output.class);
        when(rsm.submit(any())).thenReturn(CompletableFuture.completedFuture(output));

        assertEquals(RespError.withUTF8("共识冲突").toByteBuffer(),
                keyValueEngine.execute(request("get", "key"), 3, TimeUnit.SECONDS));
    }

    private ReplicateStateMachine.Input requestState(String cmd, String... args) {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        byte[] content = RespArray.with(datas).toBytes();

        return ReplicateStateMachine.Input.builder().content(content).id(1).uuid("uuid".getBytes()).build();
    }

    private byte[] request(String cmd, String... args) {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        return RespArray.with(datas).toBytes();
    }
}