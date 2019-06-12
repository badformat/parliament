package io.github.parliament.kv;

import io.github.parliament.*;
import io.github.parliament.resp.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KeyValueServerTest {
    private static KeyValueServer server;
    private static SocketChannel client;
    private static RespDecoder respDecoder = RespDecoder.create();
    private static volatile Output output = mock(Output.class);
    private static volatile Input input = mock(Input.class);

    @BeforeAll
    static void beforeAll() throws Exception {
        ReplicateStateMachine rsm = mock(ReplicateStateMachine.class);
        KeyValueEngine engine = KeyValueEngine.builder()
                .executorService(mock(ExecutorService.class))
                .persistence(mock(Persistence.class))
                .rsm(rsm)
                .build();

        when(output.getContent()).thenReturn(RespInteger.with(1).toBytes());
        when(output.getId()).thenReturn(1);
        when(output.getUuid()).thenReturn("uuid".getBytes());
        when(input.getUuid()).thenReturn("uuid".getBytes());

        when(rsm.newState(any())).thenReturn(input);

        when(rsm.submit(any())).thenReturn(CompletableFuture.completedFuture(output));

        server = KeyValueServer.builder()
                .socketAddress(new InetSocketAddress("127.0.0.1", 30000))
                .keyValueEngine(engine)
                .build();
        server.start();
        client = SocketChannel.open(server.getSocketAddress());
    }

    @AfterAll
    static void afterAll() throws IOException {
        client.close();
        server.shutdown();
    }

    /**
     * put命令，在rsm中成功达成一致，返回resp "ok"字符串，否则返回error信息。
     *
     * @throws IOException
     */
    @Test
    void handlePutRequest() throws IOException {
        RespBulkString expected = RespBulkString.with("any value".getBytes());
        when(output.getContent()).thenReturn(expected.toBytes());

        sendReq("put", "any key", "any value");
        assertEquals(expected, receiveResp());
    }

    @Test
    void handleInvalidRequest() throws IOException {
        sendReq("unknown");
        RespArray a = (RespArray) receiveResp();
        assertTrue(a.get(0) instanceof RespError);
    }

    @Test
    void handleGet() throws IOException {
        RespBulkString expected = RespBulkString.with("value1".getBytes());
        when(output.getContent()).thenReturn(expected.toBytes());

        sendReq("put", "key1", "value1");
        receiveResp();
        sendReq("get", "key1");

        assertEquals(expected, receiveResp());
    }

    @Test
    void getPeers() {
        List<InetSocketAddress> peers = Application.getPeers("127.0.0.1:50000,127.0.0.1:50001,127.0.0.1:50002");
        assertEquals(3, peers.size());
    }

    @Test
    void getMe() {
        assertEquals(new InetSocketAddress("127.0.0.1", 50002), Application.getInetSocketAddress("127.0.0.1:50002"));
    }

    private void sendReq(String cmd, String... args) throws IOException {
        List<RespData> datas = new ArrayList<>();
        datas.add(RespBulkString.with(cmd.getBytes()));
        datas.addAll(Arrays.stream(args).map(a -> RespBulkString.with(a.getBytes())).collect(Collectors.toList()));
        ByteBuffer buf = RespArray.with(datas).toByteBuffer();

        while (buf.hasRemaining()) {
            client.write(buf);
        }
    }

    private RespData receiveResp() throws IOException {
        ByteBuffer dst = ByteBuffer.allocate(10);
        int len = client.read(dst);
        while (len != -1) {
            dst.flip();
            respDecoder.decode(dst);
            RespData resp = respDecoder.get();
            if (resp != null) {
                return resp;
            }
            dst.clear();
            len = client.read(dst);
        }
        throw new IllegalStateException();
    }
}