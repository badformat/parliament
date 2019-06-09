package io.github.parliament.resp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.CompletionHandler;

public class RespWriteHandler implements CompletionHandler<Integer, RespHandlerAttachment> {
    private static final Logger logger = LoggerFactory.getLogger(RespWriteHandler.class);

    @Override
    public void completed(Integer result, RespHandlerAttachment attachment) {
        if (attachment.getByteBuffer().hasRemaining()) {
            attachment.getChannel().write(attachment.getByteBuffer(), attachment, this);
        } else {
            try {
                process(attachment);
            } finally {
                RespHandlerAttachment readAttachment = new RespHandlerAttachment(attachment);
                attachment.getChannel().read(readAttachment.getByteBuffer(), readAttachment,
                        readAttachment.getRespReadHandler());
            }
        }
    }

    @Override
    public void failed(Throwable exc, RespHandlerAttachment attachment) {
        logger.error("write handler failed.", exc);
    }

    protected void process(RespHandlerAttachment attachment) {

    }
}
