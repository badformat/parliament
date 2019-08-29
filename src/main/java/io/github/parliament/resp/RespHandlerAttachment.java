package io.github.parliament.resp;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

/**
 * 客户连接channel上挂载的附件对象，包含{@link RespReadHandler read handler}和{@link RespWriteHandler write handler}。
 * 使用{@link RespDecoder}解码。
 * @author zy
 */
public class RespHandlerAttachment {
    // 读写缓冲区
    @Getter
    private volatile ByteBuffer byteBuffer;
    // 相关socket channel
    @Getter
    private volatile AsynchronousSocketChannel channel;
    // resp协议decoder
    @Getter
    private volatile RespDecoder respDecoder;
    // 超时设置
    @Getter
    private int timeOutMills = 3000;
    // channel read handler
    @Getter
    private RespReadHandler respReadHandler;
    // channel write handler
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
