package io.github.parliament.resp;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

public class RespHandlerAttachment {
    @Getter
    private volatile ByteBuffer byteBuffer;
    @Getter
    private volatile AsynchronousSocketChannel channel;
    @Getter
    private volatile RespDecoder respDecoder;
    @Getter
    private int timeOutMills = 3000;
    @Getter
    private RespReadHandler respReadHandler;
    @Getter
    private RespWriteHandler respWriteHandler;

    public RespHandlerAttachment(AsynchronousSocketChannel channel, RespReadHandler respReadHandler, RespWriteHandler respWriteHandler) {
        this.respReadHandler = respReadHandler;
        this.respWriteHandler = respWriteHandler;
        this.channel = channel;
        this.byteBuffer = ByteBuffer.allocate(512);
        this.respDecoder = RespDecoder.create();
    }

    public RespHandlerAttachment(RespHandlerAttachment attachment, ByteBuffer buffer) {
        this.channel = attachment.getChannel();
        this.byteBuffer = buffer;
        this.respDecoder = RespDecoder.create();
        this.respWriteHandler = attachment.getRespWriteHandler();
        this.respReadHandler = attachment.getRespReadHandler();
    }

    public RespHandlerAttachment(RespHandlerAttachment attachment) {
        this.channel = attachment.getChannel();
        this.byteBuffer = ByteBuffer.allocate(512);
        this.respDecoder = RespDecoder.create();
        this.respWriteHandler = attachment.getRespWriteHandler();
        this.respReadHandler = attachment.getRespReadHandler();
    }
}
