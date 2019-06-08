package io.github.parliament.resp;

import com.google.common.primitives.Bytes;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@EqualsAndHashCode
@ToString
public class RespArray implements RespData {
    final static char           firstChar = '*';
    @val
    private      List<RespData> datas;

    public static RespArray empty() {
        return new RespArray(Collections.emptyList());
    }

    public static RespArray with(List<RespData> datas) {
        return new RespArray(new ArrayList<>(datas));
    }

    public static RespArray with(RespData... datas) {
        return new RespArray(Arrays.asList(datas));
    }

    private RespArray(List<RespData> datas) {
        this.datas = datas;
    }

    public int size() {
        return datas.size();
    }

    public <T> T get(int i) {
        return (T) datas.get(i);
    }

    public <T> List<T> getDatas() {
        return (List<T>) datas;
    }

    @Override
    public byte[] toBytes() {
        StringBuilder sb = new StringBuilder();
        sb.append(firstChar);
        sb.append(datas.size());
        sb.append("\r\n");

        byte[] bytes = sb.toString().getBytes();

        for (RespData data : datas) {
            bytes = Bytes.concat(bytes, data.toBytes());
        }

        return bytes;
    }
}
