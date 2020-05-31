package io.github.parliament.kv;

/**
 *
 * @author zy
 */
public class UnknownKeyValueCommand extends Exception {

	private static final long serialVersionUID = -8762831055496952003L;

	public UnknownKeyValueCommand(String s) {
        super(s);
    }
}