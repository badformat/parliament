package io.github.parliament.paxos;

import java.util.Arrays;

public class Proposal {
	private long round;
	private byte[] content;

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(content);
		result = prime * result + (int) (round ^ (round >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Proposal other = (Proposal) obj;
		if (!Arrays.equals(content, other.content))
			return false;
		if (round != other.round)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Proposal [round=" + round + ", content=" + Arrays.toString(content) + "]";
	}

}
