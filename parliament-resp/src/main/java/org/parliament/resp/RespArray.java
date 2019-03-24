package org.parliament.resp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.primitives.Bytes;

import lombok.Getter;
import lombok.val;

public class RespArray implements RespData {
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
        byte[] bytes = ("*" + datas.size() + "\r\n").getBytes();
        for (RespData data : datas) {
            bytes = Bytes.concat(bytes, data.toBytes());
        }

        return bytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datas);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof RespArray))
            return false;
        RespArray other = (RespArray) obj;
        return Objects.equals(datas, other.datas);
    }

    @Override
    public String toString() {
        final int maxLen = 20;
        return "RespArray [" + (datas != null ? "datas=" + datas.subList(0, Math.min(datas.size(), maxLen)) : "") + "]";
    }
}
