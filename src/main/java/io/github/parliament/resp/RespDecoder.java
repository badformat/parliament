package io.github.parliament.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * RESP协议解码器，使用自定义的{@link  ByteBuf}处理数据。
 * @author zy
 */
public class RespDecoder {

    private enum Type {
        SIMPLE_STRING,
        ERROR,
        INTEGER,
        BULK_STRING,
        ARRAY;

        static Type of(char c) {
            switch (c) {
                case '+':
                    return SIMPLE_STRING;
                case '-':
                    return ERROR;
                case ':':
                    return INTEGER;
                case '$':
                    return BULK_STRING;
                case '*':
                    return ARRAY;
                default:
                    throw new UnknownRespTypeException();
            }
        }
    }

    private enum State {
        DECODE_TYPE,
        DECODE_INLINE, // SIMPLE_STRING, ERROR, INTEGER
        DECODE_LENGTH, // BULK_STRING, ARRAY_HEADER
        DECODE_BULK_STRING_CONTENT,
    }

    private static class ArrayAggregator {
        private int            len;
        private List<RespData> children = new ArrayList<>();

        ArrayAggregator(int len) {
            this.len = len;
        }

        void addData(RespData child) {
            if (children.size() == len) {
                throw new IllegalStateException();
            }
            children.add(child);
        }

        boolean isFinished() {
            return children.size() == len;
        }

        RespArray getArray() {
            if (children.size() != len) {
                throw new IllegalStateException();
            }
            return RespArray.with(children);
        }
    }

    private Type                   type;
    private State                  state       = State.DECODE_TYPE;
    private int                    bulkLength  = -1;
    private ByteBuf                byteBuf     = ByteBuf.allocate(512);
    private Deque<ArrayAggregator> aggregators = new ArrayDeque<>();
    private List<RespData>         messages    = new ArrayList<>();

    public static RespDecoder create() {
        return new RespDecoder();
    }

    public RespDecoder decode(byte[] bytes) {
        byteBuf.writeBytes(bytes);
        decode0();
        return this;
    }

    public RespDecoder decode(ByteBuffer buf) {
        byteBuf.writeBytes(buf);
        decode0();
        return this;
    }

    private void decode0() {
        for (; ; ) {
            switch (state) {
                case DECODE_TYPE:
                    State next = decodeType();
                    if (next == null) {
                        return;
                    }
                    state = next;
                    break;
                case DECODE_INLINE:
                    RespData msg = decodeInline();
                    if (msg != null) {
                        addMessage(msg);
                    } else {
                        return;
                    }
                    break;
                case DECODE_LENGTH:
                    String len = readLine();
                    if (len != null) {
                        if (type == Type.BULK_STRING) {
                            state = State.DECODE_BULK_STRING_CONTENT;
                            bulkLength = Integer.parseInt(len);
                        } else if (type == Type.ARRAY) {
                            int length = Integer.parseInt(len);
                            if (length == 0) {
                                addMessage(RespArray.empty());
                            } else {
                                aggregators.push(new ArrayAggregator(length));
                            }
                            state = State.DECODE_TYPE;
                        }
                    } else {
                        return;
                    }
                    break;
                case DECODE_BULK_STRING_CONTENT:
                    RespBulkString bulkString = decodeBulkString();
                    if (bulkString != null) {
                        addMessage(bulkString);
                    } else {
                        return;
                    }
                    break;
                default:
                    throw new UnknownDecodeStateException();
            }
        }
    }

    private RespBulkString decodeBulkString() {
        if (bulkLength == -1) {
            return RespBulkString.nullBulkString();
        }
        if (byteBuf.readableBytes() >= bulkLength + 2) {
            byte[] bytes = new byte[bulkLength];
            byteBuf.readBytes(bytes);
            Preconditions.checkState(byteBuf.readByte() == '\r');
            Preconditions.checkState(byteBuf.readByte() == '\n');
            return RespBulkString.with(bytes);
        }
        return null;
    }

    private State decodeType() {
        if (!byteBuf.isReadable()) {
            return null;
        }
        char t = (char) byteBuf.readByte();
        switch (t) {
            case RespSimpleString.firstChar:
            case RespError.firstChar:
            case RespInteger.firstChar:
                type = Type.of(t);
                return State.DECODE_INLINE;
            case RespBulkString.firstChar:
            case RespArray.firstChar:
                type = Type.of(t);
                return State.DECODE_LENGTH;
            default:
                throw new UnknownRespTypeException();
        }
    }

    private RespData decodeInline() {
        String s = readLine();
        if (s != null) {
            switch (type) {
                case SIMPLE_STRING:
                    return RespSimpleString.withUTF8(s);
                case ERROR:
                    return RespError.withUTF8(s);
                case INTEGER:
                    return RespInteger.with(s);
                default:
                    throw new UnknownRespTypeException();
            }
        }

        return null;
    }

    private String readLine() {
        int i = byteBuf.indexOf(byteBuf.getReaderIndex(), byteBuf.getWriterIndex(), (byte) '\n');
        if (i == -1) {
            return null;
        }
        Preconditions.checkState(byteBuf.getByte(i - 1) == '\r', "not found \\r in line");

        byte[] bytes = new byte[i - 1 - byteBuf.getReaderIndex()];
        byteBuf.readBytes(bytes);
        byteBuf.readByte();
        byteBuf.readByte();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void addMessage(RespData data) {
        state = State.DECODE_TYPE;
        bulkLength = -1;
        if (aggregators.isEmpty()) {
            messages.add(data);
            return;
        }

        ArrayAggregator aggregator = aggregators.getFirst();
        aggregator.addData(data);
        if (aggregator.isFinished()) {
            aggregators.pop();
            addMessage(aggregator.getArray());
        }
    }

    public <T> T get() {
        return messages.size() == 0 ? null : (T) messages.remove(0);
    }
}