package org.parliament.resp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import lombok.Getter;

public class RespInteger extends RespString {
    @Getter
    private Integer n = null;

    public RespInteger(int i) {
        this(String.valueOf(i), StandardCharsets.UTF_8);
    }

    public RespInteger(String content, Charset charset) {
        super(':', content, charset);
        n = Integer.valueOf(content);
    }

    @Override
    public String toString() {
        return "RespInteger [" + (n != null ? "n=" + n + ", " : "") + "getFirstByte()=" + getFirstByte() + ", "
                + (getContent() != null ? "getContent()=" + getContent() + ", " : "")
                + (getCharset() != null ? "getCharset()=" + getCharset() : "") + "]";
    }

}
