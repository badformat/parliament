package io.github.parliament.resp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Bytes;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.val;

@EqualsAndHashCode
@ToString
public class RespArray implements RespData {
    public final static char firstByte = '*';
    @Getter
    @val
    private List<RespData> datas;

    public static RespArray empty() {
        return new RespArray(Collections.emptyList());
    }

    public static RespArray with(List<RespData> datas) {
        return new RespArray(Collections.unmodifiableList(datas));
    }

    public static RespArray with(RespData... datas) {
        return new RespArray(Arrays.asList(datas));
    }

    RespArray(List<RespData> emptyList) {
        this.datas = emptyList;
    }

    public int size() {
        return datas.size();
    }

    public RespData get(int i) {
        return datas.get(i);
    }

    @Override
    public byte[] toBytes() {
        StringBuilder sb = new StringBuilder();
        sb.append(firstByte);
        sb.append(datas.size());
        sb.append("\r\n");

        byte[] bytes = sb.toString().getBytes();

        for (RespData data : datas) {
            bytes = Bytes.concat(bytes, data.toBytes());
        }

        return bytes;
    }
}
