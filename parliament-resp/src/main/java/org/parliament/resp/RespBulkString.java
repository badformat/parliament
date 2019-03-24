package org.parliament.resp;

import java.util.Arrays;
import java.util.Objects;

import com.google.common.primitives.Bytes;

import lombok.Getter;

public class RespBulkString implements RespData {
    @Getter
    private int length = 0;
    @Getter
    private byte[] content;
    private byte firstByte = '$';

    public RespBulkString(byte[] content) {
        this.content = content;
        if (content == null) {
            this.length = -1;
        } else {
            this.length = this.content.length;
        }
    }

    @Override
    public byte[] toBytes() {
        byte[] bytes = { firstByte };

        bytes = Bytes.concat(bytes, String.valueOf(length).getBytes(), "\r\n".getBytes());
        if (this.content != null) {
            bytes = Bytes.concat(bytes, content, "\r\n".getBytes());
        }
        return bytes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(content);
        result = prime * result + Objects.hash(firstByte, length);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof RespBulkString))
            return false;
        RespBulkString other = (RespBulkString) obj;
        return Arrays.equals(content, other.content) && firstByte == other.firstByte && length == other.length;
    }

    @Override
    public String toString() {
        final int maxLen = 20;
        return "BulkString [length=" + length + ", "
                + (content != null
                        ? "content=" + Arrays.toString(Arrays.copyOf(content, Math.min(content.length, maxLen))) + ", "
                        : "")
                + "firstByte=" + firstByte + "]";
    }

}
