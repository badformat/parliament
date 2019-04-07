package io.github.parliament.server;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import io.github.parliament.paxos.acceptor.Accept;
import io.github.parliament.paxos.acceptor.Prepare;
import org.parliament.resp.RespArray;
import org.parliament.resp.RespBulkString;
import org.parliament.resp.RespError;
import org.parliament.resp.RespSimpleString;

/**
 *
 * @author zy
 */
public class AcceptorResponseSerializer {
    public ByteBuffer serializerPrepareResponse(Prepare<String> prepare) throws IOException {
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

    public ByteBuffer serializerAcceptResponse(Accept<String> accept) {
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

    public ByteBuffer serializeErrorResponse(String msg) {
        RespError n = RespError.withUTF8(msg);
        RespArray a = RespArray.with(n);
        return a.toByteBuffer();

    }

    public ByteBuffer serializerDecideResponse() {
        RespArray a;
        RespSimpleString ok = RespSimpleString.withUTF8("ok");
        a = RespArray.with(ok);
        return a.toByteBuffer();
    }
}