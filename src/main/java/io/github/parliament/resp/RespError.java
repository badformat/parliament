package io.github.parliament.resp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RespError extends RespString {
    static final char firstChar = '-';

    public static RespError withUTF8(String msg) {
        return new RespError(msg, StandardCharsets.UTF_8);
    }

    public RespError(String content, Charset charset) {
        super(content, charset);
    }

    @Override
    char getFirstChar() {
        return firstChar;
    }

    @Override
    public String toString() {
        return "RespError [getFirstChar()=" + getFirstChar() + ", "
                + (getContent() != null ? "getContent()=" + getContent() + ", " : "")
                + (getCharset() != null ? "getCharset()=" + getCharset() : "") + "]";
    }
}
