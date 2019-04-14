package io.github.parliament.paxos;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 *
 * @author zy
 */
@Builder
@ToString
@EqualsAndHashCode
public class Proposal {
    @Getter
    private int    round;
    @Getter
    private byte[] agreement;
}