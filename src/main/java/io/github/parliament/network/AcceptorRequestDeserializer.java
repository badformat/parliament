package io.github.parliament.network;

import java.io.IOException;
import java.nio.channels.ByteChannel;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespBulkString;
import io.github.parliament.resp.RespData;
import io.github.parliament.resp.RespInteger;
import io.github.parliament.resp.RespSimpleString;
import io.github.parliament.resp.reader.RespParser;

/**
 *
 * @author zy
 */
public class AcceptorRequestDeserializer {
    public enum Command {
        prepare,
        accept,
        decide
    }

    @Value
    @EqualsAndHashCode
    @ToString
    @Builder
    public static class AcceptorRequest {
        @Getter
        Command cmd;
        @Getter
        int     round;
        @Getter
        String  n;
        @Getter
        byte[]  v;
    }

    public AcceptorRequest deserializeRequest(ByteChannel channel) throws IOException {
        RespParser respParser = RespParser.create(channel);
        RespArray a = respParser.getAsArray();
        String cmd = ((RespSimpleString) a.get(0)).getContent();
        Integer round = ((RespInteger) a.get(1)).getN();
        switch (cmd) {
            case "prepare":
                return AcceptorRequest.builder()
                        .cmd(Command.valueOf(cmd))
                        .round(round)
                        .n(((RespSimpleString) a.get(2)).getContent())
                        .build();
            case "accept":
                return AcceptorRequest.builder()
                        .cmd(Command.valueOf(cmd)).round(round)
                        .n(((RespSimpleString) a.get(2)).getContent())
                        .v(((RespBulkString) a.get(3)).getContent())
                        .build();
            case "decide":
                RespData va = a.get(2);
                if (!(va instanceof RespBulkString)) {
                    throw new IllegalStateException("decide value is not bulk string.");
                }
                RespBulkString content = (RespBulkString) va;
                if (content.getLength() <= 0) {
                    throw new IllegalStateException("decide value length is " + content.getLength());
                }

                return AcceptorRequest.builder()
                        .cmd(Command.valueOf(cmd))
                        .round(round)
                        .v(((RespBulkString) a.get(2)).getContent())
                        .build();
            default:
                throw new IllegalStateException();
        }
    }
}