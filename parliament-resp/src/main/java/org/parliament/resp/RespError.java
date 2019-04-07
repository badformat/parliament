package org.parliament.resp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RespError extends RespString {
    public static final char firstByte = '-';

    public static RespError withUTF8(String msg) {
        return new RespError(msg, StandardCharsets.UTF_8);
    }

    public RespError(String content, Charset charset) {
        super(firstByte, content, charset);
    }

    @Override
    public String toString() {
        return "RespError [getFirstByte()=" + getFirstByte() + ", "
                + (getContent() != null ? "getContent()=" + getContent() + ", " : "")
                + (getCharset() != null ? "getCharset()=" + getCharset() : "") + "]";
    }
}
