package io.github.parliament.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.CompletionHandler;

public class WriteHandler implements CompletionHandler<Integer, KeyValueEngine> {
    private static final Logger logger = LoggerFactory.getLogger(WriteHandler.class);

    private ReadHandler readHandler;

    WriteHandler(ReadHandler readHandler) {
        this.readHandler = readHandler;
    }

    @Override
    public void completed(Integer result, KeyValueEngine engine) {
        readHandler.getChannel().read(readHandler.getByteBuffer(), engine, readHandler);
    }

    @Override
    public void failed(Throwable exc, KeyValueEngine attachment) {
        logger.error("kv server in write failed.", exc);
    }
}
