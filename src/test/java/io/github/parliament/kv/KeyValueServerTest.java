package io.github.parliament.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespDecoder;
import io.github.parliament.resp.RespError;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueServerTest {
    private static KeyValueServer server;
    private static SocketChannel  client;

    @BeforeAll
    static void beforeAll() throws Exception {
        KeyValueEngine memKeyValueEngine = new MemoryKeyValueEngine();

        server = KeyValueServer.builder()
                .socketAddress(new InetSocketAddress("127.0.0.1", 30000))
                .keyValueEngine(memKeyValueEngine)
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
        sendReq("put", "any key", "any value");

        assertEquals(RespBulkString.with("any value".getBytes()), receiveResp());
    }

    @Test
    void handleInvalidRequest() throws IOException {
        sendReq("unknown");
        assertTrue(receiveResp() instanceof RespError);
    }

    @Test
    void handleGet() throws IOException {
        sendReq("put", "key1", "value1");
        receiveResp();
        sendReq("get", "key1");
        assertEquals(RespBulkString.with("value1".getBytes()), receiveResp());
    }

    @Test
    void getPeers() {
        List<InetSocketAddress> peers = KeyValueServer.getPeers("127.0.0.1:50000,127.0.0.1:50001,127.0.0.1:50002");
        assertEquals(3, peers.size());
    }

    @Test
    void getMe() {
        assertEquals(new InetSocketAddress("127.0.0.1", 50002), KeyValueServer.getInetSocketAddress("127.0.0.1:50002"));
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
        ByteBuffer dst = ByteBuffer.allocate(1024);
        RespDecoder respDecoder = RespDecoder.create();
        while (client.read(dst) != -1) {
            dst.flip();
            respDecoder.decode(dst);
            RespData resp = respDecoder.get();
            if (resp != null) {
                return resp;
            }
        }
        throw new IllegalStateException();
    }
}