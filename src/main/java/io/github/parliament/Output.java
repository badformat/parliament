package io.github.parliament;

import lombok.*;

@Builder
@EqualsAndHashCode
@ToString
public class Output {
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
     * 状态机输出的内容
     */
    @Getter
    @NonNull
    private byte[] content;
}
