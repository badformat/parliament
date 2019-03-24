package org.parliament.resp;

import java.nio.charset.Charset;

public class RespError extends RespString {
    public RespError(String content, Charset charset) {
        super('-', content, charset);
    }

    @Override
    public String toString() {
        return "RespError [getFirstByte()=" + getFirstByte() + ", "
                + (getContent() != null ? "getContent()=" + getContent() + ", " : "")
                + (getCharset() != null ? "getCharset()=" + getCharset() : "") + "]";
    }
}
