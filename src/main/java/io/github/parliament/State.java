package io.github.parliament;

import lombok.*;

import java.io.*;

@EqualsAndHashCode
@ToString
@Builder
public class State implements Serializable {
    final static long serialVersionUID = 1L;
    /**
     * 状态机事件id，在状态流中的唯一标识
     */
    @Getter
    @NonNull
    private Integer id;
    /**
     * 身份tag，记录来自哪个事件发送方
     */
    @Getter
    @NonNull
    private byte[] uuid;
    /**
     * 该事件的内容
     */
    @Getter
    @NonNull
    private byte[] content;

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private byte[] output;

    /**
     * 事件内容是否已被处理
     */
    @Getter
    @Setter(AccessLevel.PACKAGE)
    @Builder.Default
    private boolean processed = false;

    public static byte[] serialize(State state) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(state);
        }
        return os.toByteArray();
    }

    public static State deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(is)) {
            return (State) ois.readObject();
        }
    }
}
