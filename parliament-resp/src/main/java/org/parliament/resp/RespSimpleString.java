package org.parliament.resp;

import java.nio.charset.Charset;

public class RespSimpleString extends RespString {
    public RespSimpleString(String content, Charset charset) {
        super('+', content, charset);
    }

    @Override
    public String toString() {
        return "RespSimpleString [getFirstByte()=" + getFirstByte() + ", "
                + (getContent() != null ? "getContent()=" + getContent() + ", " : "")
                + (getCharset() != null ? "getCharset()=" + getCharset() : "") + "]";
    }
}
