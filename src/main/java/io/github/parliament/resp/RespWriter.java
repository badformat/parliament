package io.github.parliament.resp;

import java.nio.channels.WritableByteChannel;
import java.util.List;

public class RespWriter {
    private WritableByteChannel byteChannel;

    public static RespWriter with(WritableByteChannel byteChannel) {
        return new RespWriter(byteChannel);
    }

    private RespWriter(WritableByteChannel byteChannel) {
        this.byteChannel = byteChannel;
    }

    public void append(RespData respData) throws Exception {
        byteChannel.write(respData.toByteBuffer());
    }

    public void append(RespData... respDatas) throws Exception {
        for (RespData respData : respDatas) {
            byteChannel.write(respData.toByteBuffer());
        }
    }

    public void append(List<RespData> respDatas) throws Exception {
        for (RespData respData : respDatas) {
            byteChannel.write(respData.toByteBuffer());
        }
    }
}
