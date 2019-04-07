package io.github.parliament.server;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import org.parliament.resp.RespArray;
import org.parliament.resp.RespBulkString;
import org.parliament.resp.RespInteger;
import org.parliament.resp.RespSimpleString;

/**
 *
 * @author zy
 */
class AcceptorRequestSerializer {
    ByteBuffer serializePrepare(int round, String n) {
        RespSimpleString cmd = RespSimpleString.withUTF8("prepare");
        RespInteger pr = RespInteger.with(round);
        RespSimpleString pn = RespSimpleString.withUTF8(n);
        RespArray a = RespArray.with(cmd, pr, pn);

        return a.toByteBuffer();
    }

    ByteBuffer serializeAccept(int round, String n, byte[] value) {
        RespSimpleString cmd = RespSimpleString.withUTF8("accept");
        RespInteger pr = RespInteger.with(round);
        RespSimpleString pn = RespSimpleString.withUTF8(n);
        RespBulkString va = RespBulkString.with(value);
        RespArray a = RespArray.with(cmd, pr, pn, va);

        return a.toByteBuffer();
    }

    ByteBuffer serializeDecide(int round, byte[] value) {
        Preconditions.checkNotNull(value, "serialize null decide value");
        RespSimpleString cmd = RespSimpleString.withUTF8("decide");
        RespInteger pr = RespInteger.with(round);
        RespBulkString va = RespBulkString.with(value);
        RespArray a = RespArray.with(cmd, pr, va);

        return a.toByteBuffer();
    }
}