package io.github.parliament;

import java.util.Arrays;
import java.util.Objects;

public class Proposal {
    private long round;
    private byte[] content;
    private byte[] agreement;

    public Proposal(long round, byte[] content) {
        this.round = round;
        this.content = content;
    }

    public long getRound() {
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(agreement);
        result = prime * result + Arrays.hashCode(content);
        result = prime * result + Objects.hash(round);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Proposal))
            return false;
        Proposal other = (Proposal) obj;
        return Arrays.equals(agreement, other.agreement) && Arrays.equals(content, other.content)
                && round == other.round;
    }

    @Override
    public String toString() {
        final int maxLen = 20;
        return "Proposal [round=" + round + ", "
                + (content != null
                        ? "content=" + Arrays.toString(Arrays.copyOf(content, Math.min(content.length, maxLen))) + ", "
                        : "")
                + (agreement != null
                        ? "agreement=" + Arrays.toString(Arrays.copyOf(agreement, Math.min(agreement.length, maxLen)))
                        : "")
                + "]";
    }

}
