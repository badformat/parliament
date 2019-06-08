package io.github.parliament.paxos.client;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.resp.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Optional;

/**
 * @author zy
 */
public class ClientCodec {
    public ByteBuffer encodePrepare(int round, String n) {
        RespSimpleString cmd = RespSimpleString.withUTF8("prepare");
        RespInteger pr = RespInteger.with(round);
        RespSimpleString pn = RespSimpleString.withUTF8(n);
        RespArray a = RespArray.with(cmd, pr, pn);

        return a.toByteBuffer();
    }

    public ByteBuffer encodeAccept(int round, String n, byte[] value) {
        RespSimpleString cmd = RespSimpleString.withUTF8("accept");
        RespInteger pr = RespInteger.with(round);
        RespSimpleString pn = RespSimpleString.withUTF8(n);
        RespBulkString va = RespBulkString.with(value);
        RespArray a = RespArray.with(cmd, pr, pn, va);

        return a.toByteBuffer();
    }

    public ByteBuffer encodeDecide(int round, byte[] value) {
        Preconditions.checkNotNull(value, "serialize null decide value");
        RespSimpleString cmd = RespSimpleString.withUTF8("decide");
        RespInteger pr = RespInteger.with(round);
        RespBulkString va = RespBulkString.with(value);
        RespArray a = RespArray.with(cmd, pr, va);

        return a.toByteBuffer();
    }

    public ByteBuffer encodeMax() {
        RespSimpleString cmd = RespSimpleString.withUTF8("max");
        return RespArray.with(cmd).toByteBuffer();
    }

    public ByteBuffer encodeDone() {
        RespSimpleString cmd = RespSimpleString.withUTF8("done");
        return RespArray.with(cmd).toByteBuffer();
    }

    public ByteBuffer encodePull(int round) {
        RespSimpleString cmd = RespSimpleString.withUTF8("pull");
        RespInteger r = RespInteger.with(round);
        return RespArray.with(cmd, r).toByteBuffer();
    }

    public Prepare decodePrepare(ByteChannel remote, String n) throws IOException {
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

    public Accept decodeAccept(ByteChannel remote, String n) throws IOException {
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

    public void decodeDecide(ByteChannel remote) throws IOException {
        RespParser respParser = RespParser.create(remote);
        RespArray array = respParser.getAsArray();
        if (array.get(0) instanceof RespError) {
            throw new IllegalStateException(((RespError) array.get(0)).getContent());
        }
    }

    public int decodeMax(SocketChannel remote) throws IOException {
        RespParser respParser = RespParser.create(remote);
        return respParser.getAsInteger().getN();
    }

    public int decodeDone(SocketChannel remote) throws IOException {
        RespParser respParser = RespParser.create(remote);
        return respParser.getAsInteger().getN();
    }

    public Optional<byte[]> decodePull(int round, SocketChannel remote) throws IOException {
        RespParser respParser = RespParser.create(remote);
        RespArray array = respParser.getAsArray();
        if (array.size() == 0) {
            return Optional.empty();
        }

        if (array.get(0) instanceof RespError) {
            throw new IllegalStateException(((RespError) array.get(0)).getContent());
        }

        int rs = ((RespInteger) array.get(0)).getN();
        Preconditions.checkState(rs == round);
        byte[] agreement = ((RespBulkString) array.get(1)).getContent();
        return Optional.of(agreement);
    }
}