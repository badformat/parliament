package io.github.parliament.paxos.server;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;
import io.github.parliament.resp.*;
import lombok.*;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author zy
 */
public class ServerCodec {

    public enum Command {
        prepare,
        accept,
        decide,
        done,
        min,
        max,
        pull
    }

    @Value
    @EqualsAndHashCode
    @ToString
    @Builder
    static class Request {
        @Getter
        Command cmd;
        @Getter
        int round;
        @Getter
        String n;
        @Getter
        byte[] v;
    }

    public Request decode(RespArray a) {
        String cmd = ((RespSimpleString) a.get(0)).getContent();
        switch (cmd) {
            case "prepare":
                Integer round = ((RespInteger) a.get(1)).getN();
                return Request.builder()
                        .cmd(Command.valueOf(cmd))
                        .round(round)
                        .n(((RespSimpleString) a.get(2)).getContent())
                        .build();
            case "accept":
                round = ((RespInteger) a.get(1)).getN();
                return Request.builder()
                        .cmd(Command.valueOf(cmd)).round(round)
                        .n(((RespSimpleString) a.get(2)).getContent())
                        .v(((RespBulkString) a.get(3)).getContent())
                        .build();
            case "decide":
                round = ((RespInteger) a.get(1)).getN();
                RespData va = a.get(2);
                if (!(va instanceof RespBulkString)) {
                    throw new IllegalStateException("decide value is not bulk string.");
                }
                RespBulkString content = (RespBulkString) va;
                if (content.getLength() <= 0) {
                    throw new IllegalStateException("decide value length is " + content.getLength());
                }

                return Request.builder()
                        .cmd(Command.valueOf(cmd))
                        .round(round)
                        .v(((RespBulkString) a.get(2)).getContent())
                        .build();
            case "max":
            case "min":
            case "done":
                return Request.builder().cmd(Command.valueOf(cmd)).build();
            case "pull":
                round = ((RespInteger) a.get(1)).getN();
                return Request.builder().cmd(Command.valueOf(cmd)).round(round).build();
            default:
                throw new IllegalStateException();
        }
    }

    public ByteBuffer encodePrepare(Prepare prepare) {
        RespSimpleString n = RespSimpleString.withUTF8(prepare.getN());
        RespArray a;

        if (prepare.isOk()) {
            RespSimpleString ok = RespSimpleString.withUTF8("ok");
            if (prepare.getNa() != null) {
                Preconditions.checkNotNull(prepare.getVa());
                RespSimpleString na = RespSimpleString.withUTF8(prepare.getNa());
                RespBulkString va = RespBulkString.with(prepare.getVa());
                a = RespArray.with(ok, n, na, va);
            } else {
                a = RespArray.with(ok, n);
            }
        } else {
            a = RespArray.with(RespSimpleString.withUTF8("reject"), n);
        }
        return a.toByteBuffer();
    }

    public ByteBuffer encodeAccept(Accept accept) {
        RespSimpleString n = RespSimpleString.withUTF8(accept.getN());
        RespArray a;
        if (accept.isOk()) {
            RespSimpleString ok = RespSimpleString.withUTF8("ok");
            a = RespArray.with(ok, n);
        } else {
            a = RespArray.with(RespSimpleString.withUTF8("reject"), n);
        }
        return a.toByteBuffer();
    }

    public ByteBuffer encodeError(String msg) {
        RespError n = RespError.withUTF8(msg);
        RespArray a = RespArray.with(n);
        return a.toByteBuffer();

    }

    public ByteBuffer encodeDecide() {
        RespArray a;
        RespSimpleString ok = RespSimpleString.withUTF8("ok");
        a = RespArray.with(ok);
        return a.toByteBuffer();
    }

    ByteBuffer encodeInt(int max) {
        return RespInteger.with(max).toByteBuffer();
    }

    ByteBuffer encodeProposal(int round, Optional<byte[]> proposal) {
        if (!proposal.isPresent()) {
            return RespArray.empty().toByteBuffer();
        }
        return RespArray.with(RespInteger.with(round), RespBulkString.with(proposal.get()))
                .toByteBuffer();
    }
}