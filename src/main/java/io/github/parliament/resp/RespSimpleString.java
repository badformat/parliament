package io.github.parliament.resp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class RespSimpleString extends RespString {
    static final char firstChar = '+';

    static public RespSimpleString withUTF8(String content) {
        return new RespSimpleString(content, StandardCharsets.UTF_8);
    }

    static public RespSimpleString with(String content, Charset charset) {
        return new RespSimpleString(content, charset);
    }

    public RespSimpleString(String content, Charset charset) {
        super(content, charset);
    }

    @Override
    char getFirstChar() {
        return firstChar;
    }
}
