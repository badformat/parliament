package io.github.parliament;

import lombok.*;

import java.io.*;

@EqualsAndHashCode
@ToString
@Builder
public class Input implements Serializable {
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

    public static byte[] serialize(Input input) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(input);
        }
        return os.toByteArray();
    }

    public static Input deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(is)) {
            return (Input) ois.readObject();
        }
    }
}
