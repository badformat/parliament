package io.github.parliament.rsm;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 *
 * @author zy
 */
@EqualsAndHashCode
@ToString
@Builder
public class StateMachineEvent {
    enum Status {
        unknown,
        deleted,
        decided
    }

    static final StateMachineEvent deleted = StateMachineEvent.builder().status(Status.deleted).build();
    static final StateMachineEvent unknown = StateMachineEvent.builder().status(Status.unknown).build();

    @Getter
    private int    round;
    @Getter
    private Status status;
    @Getter
    private byte[] agreement;
}