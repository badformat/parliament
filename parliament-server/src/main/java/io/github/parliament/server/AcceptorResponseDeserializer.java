package io.github.parliament.server;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.util.Objects;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;
import org.parliament.resp.RespArray;
import org.parliament.resp.RespBulkString;
import org.parliament.resp.RespData;
import org.parliament.resp.RespError;
import org.parliament.resp.RespSimpleString;
import org.parliament.resp.reader.RespParser;

/**
 *
 * @author zy
 */
class AcceptorResponseDeserializer {
    Prepare<String> deserializePrepare(ByteChannel remote, String n) throws IOException {
        RespParser respParser = RespParser.create(remote);
        RespArray array = respParser.getAsArray();
        if (array.get(0) instanceof RespError) {
            throw new IllegalStateException(((RespError) array.get(0)).getContent());
        }
        String status = ((RespSimpleString) array.get(0)).getContent();
        String rn = ((RespSimpleString) array.get(1)).getContent();

        Preconditions.checkState(Objects.equals(rn, n));
        if (Objects.equals(status, "ok")) {
            if (array.size() > 2) {
                String rna = ((RespSimpleString) array.get(2)).getContent();
                byte[] rva = ((RespBulkString) array.get(3)).getContent();
                return Prepare.ok(rn, rna, rva);
            } else {
                return Prepare.ok(rn, null, null);
            }
        }
        return Prepare.reject(rn);
    }

    Accept<String> deserializeAccept(ByteChannel remote, String n) throws IOException {
        RespParser respParser = RespParser.create(remote);
        RespArray array = respParser.getAsArray();
        if (array.get(0) instanceof RespError) {
            throw new IllegalStateException(((RespError) array.get(0)).getContent());
        }
        String rs = ((RespSimpleString) array.get(0)).getContent();
        String rn = ((RespSimpleString) array.get(1)).getContent();

        Preconditions.checkState(Objects.equals(rn, n));
        if (Objects.equals(rs, "ok")) {
            return Accept.ok(rn);
        }
        return Accept.reject(rn);
    }

    void deserializeDecide(ByteChannel remote) throws IOException {
        RespParser respParser = RespParser.create(remote);
        RespArray array = respParser.getAsArray();
        if (array.get(0) instanceof RespError) {
            throw new IllegalStateException(((RespError) array.get(0)).getContent());
        }
    }
}