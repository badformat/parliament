package io.github.parliament;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class Proposal implements Serializable {
    private static final long serialVersionUID = 1294715417084364029L;
    private int round;
    private byte[] content;
    private byte[] agreement;

    public Proposal(int round, byte[] content) {
        this.round = round;
        this.content = content;
    }

    public int getRound() {
        return round;
    }

    public byte[] getContent() {
        return content;
    }

    public byte[] getAgreement() {
        return agreement;
    }

    void setAgreement(byte[] agreement) {
        this.agreement = agreement;
    }
}
