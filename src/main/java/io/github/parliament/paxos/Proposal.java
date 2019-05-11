package io.github.parliament.paxos;

import java.io.Serializable;
import java.util.concurrent.Future;

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
public class Proposal implements Serializable {
    static final long           serialVersionUID = 42L;
    @Getter
    private      int            round;
    @Getter
    private      Future<byte[]> agreement;
}