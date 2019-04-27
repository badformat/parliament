package io.github.parliament.resp;

import java.nio.charset.Charset;

import com.google.common.base.Preconditions;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
abstract class RespString implements RespData {
    @Getter
    private String  content;
    @Getter
    private Charset charset;

    public RespString(String content, Charset charset) {
        Preconditions.checkArgument(!content.contains("\r"), "resp simple string不能包含\\r");
        Preconditions.checkArgument(!content.contains("\n"), "resp simple string不能包含\\n");
        this.content = content;
        this.charset = charset;
    }

    abstract char getFirstChar();

    @Override
    public byte[] toBytes() {
        return ((char) getFirstChar() + content + "\r\n").getBytes(charset);
    }
}
