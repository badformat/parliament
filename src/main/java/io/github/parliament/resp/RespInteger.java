package io.github.parliament.resp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class RespInteger extends RespString {
    public final static char    firstByte = ':';
    @Getter
    private             Integer n         = null;

    public static RespInteger with(int i) {
        return new RespInteger(i);
    }

    public static RespInteger with(String i) {
        return new RespInteger(Integer.valueOf(i));
    }

    private RespInteger(int i) {
        this(String.valueOf(i), StandardCharsets.UTF_8);
    }

    public RespInteger(String content, Charset charset) {
        super(firstByte, content, charset);
        n = Integer.valueOf(content);
    }

}
