package io.github.parliament.resp;

import com.google.common.primitives.Bytes;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

@EqualsAndHashCode
@ToString
@Value
public class RespBulkString implements RespData {
    public static final char firstChar = '$';

    private final static RespBulkString NULL = new RespBulkString(null);
    @Getter
    private              int            length;
    @Getter
    private              byte[]         content;

    public static RespBulkString with(byte[] content) {
        return new RespBulkString(content);
    }

    public RespBulkString(byte[] content) {
        this.content = content;
        if (content == null) {
            this.length = -1;
        } else {
            this.length = this.content.length;
        }
    }

    public static RespBulkString nullBulkString() {
        return NULL;
    }

    @Override
    public byte[] toBytes() {
        StringBuilder sb = new StringBuilder();
        sb.append(firstChar);
        sb.append(length);
        sb.append("\r\n");
        byte[] bytes = sb.toString().getBytes();

        if (this.content != null) {
            bytes = Bytes.concat(bytes, content, "\r\n".getBytes());
        }
        return bytes;
    }
}
