package org.parliament.resp;

import java.nio.charset.Charset;
import java.util.Objects;

import com.google.common.base.Preconditions;

import lombok.Getter;

class RespString implements RespData {
    @Getter
    private char firstByte;
    @Getter
    private String content;
    @Getter
    private Charset charset;

    public RespString(char firstByte, String content, Charset charset) {
        Preconditions.checkArgument(!content.contains("\r"), "resp simple string不能包含\\r");
        Preconditions.checkArgument(!content.contains("\n"), "resp simple string不能包含\\n");
        this.content = content;
        this.charset = charset;
        this.firstByte = firstByte;
    }

    @Override
    public byte[] toBytes() {
        return (firstByte + content + "\r\n").getBytes();
    }

    @Override
    public int hashCode() {
        return Objects.hash(charset, content, firstByte);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof RespString))
            return false;
        RespString other = (RespString) obj;
        return Objects.equals(charset, other.charset) && Objects.equals(content, other.content)
                && firstByte == other.firstByte;
    }

    @Override
    public String toString() {
        return "RespString [firstByte=" + firstByte + ", " + (content != null ? "content=" + content + ", " : "")
                + (charset != null ? "charset=" + charset : "") + "]";
    }

}
