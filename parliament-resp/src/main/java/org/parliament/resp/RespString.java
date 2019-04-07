package org.parliament.resp;

import java.nio.charset.Charset;
import java.util.Objects;

import com.google.common.base.Preconditions;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
class RespString implements RespData {
    @Getter
    private char    firstByte;
    @Getter
    private String  content;
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
        return ((char) firstByte + content + "\r\n").getBytes(charset);
    }
}
