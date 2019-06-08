package io.github.parliament.paxos.server;

import io.github.parliament.paxos.client.ClientCodec;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespDecoder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerCodecTest {
    private static ServerCodec serverCodec;
    private static ClientCodec clientCodec;
    private static RespDecoder decoder;
    private static byte[] value = "value".getBytes();

    @BeforeAll
    static void beforeAll() {
        serverCodec = new ServerCodec();
        clientCodec = new ClientCodec();
        decoder = new RespDecoder();
    }

    @Test
    void prepare() {
        ByteBuffer buf = clientCodec.encodePrepare(1, "n");
        decoder.decode(buf);
        RespArray array = decoder.get();
        ServerCodec.Request req = serverCodec.decode(array);
        assertEquals("n", req.getN());
        assertEquals(1, req.getRound());
        assertEquals(ServerCodec.Command.prepare, req.getCmd());
    }

    @Test
    void accept() {
        ByteBuffer buf = clientCodec.encodeAccept(1, "n", value);
        ServerCodec.Request req = serverCodec.decode(decoder.decode(buf).get());
        assertEquals(ServerCodec.Command.accept, req.getCmd());
        assertEquals("n", req.getN());
        assertEquals(1, req.getRound());
        assertArrayEquals(value, req.getV());
    }

    @Test
    void decide() {
        ByteBuffer buf = clientCodec.encodeDecide(1, value);
        ServerCodec.Request req = serverCodec.decode(decoder.decode(buf).get());
        assertEquals(ServerCodec.Command.decide, req.getCmd());
        assertEquals(1, req.getRound());
        assertArrayEquals(value, req.getV());
    }

    @Test
    void all() {
        prepare();
        accept();
        decide();
    }
}