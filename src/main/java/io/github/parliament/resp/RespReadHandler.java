package io.github.parliament.resp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

/**
 * @author zy
 */
public abstract class RespReadHandler implements CompletionHandler<Integer, RespHandlerAttachment> {
    private static final Logger logger = LoggerFactory.getLogger(RespReadHandler.class);

    @Override
    public void completed(Integer result, RespHandlerAttachment attachment) {
        if (result == -1) {
            return;
        }
        ByteBuffer response = null;
        AsynchronousSocketChannel channel = attachment.getChannel();
        ByteBuffer buffer = attachment.getByteBuffer();
        RespDecoder decoder = attachment.getRespDecoder();
        try {
            buffer.flip();
            decoder.decode(buffer);

            RespArray request = decoder.get();
            if (request != null) {
                response = process(attachment, request);
            } else {
                return;
            }
        } catch (Exception e) {
            logger.error("read handler exception in completed().", e);
            response = RespError.withUTF8("read handler exception:" + e.getClass().getName()).toByteBuffer();
        } finally {
            if (response != null) {
                RespHandlerAttachment writeAttachment = new RespHandlerAttachment(attachment, response);
                channel.write(writeAttachment.getByteBuffer(), writeAttachment, writeAttachment.getRespWriteHandler());
            }
        }
    }

    @Override
    public void failed(Throwable exc, RespHandlerAttachment attachment) {
        logger.error("read handler failed.", exc);
    }

    protected abstract ByteBuffer process(RespHandlerAttachment attachment, RespArray request) throws Exception;
}