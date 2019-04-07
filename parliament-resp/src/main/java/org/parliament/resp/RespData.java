package org.parliament.resp;

import java.nio.ByteBuffer;

public interface RespData {

    byte[] toBytes();

    default ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(toBytes());
    }
}
