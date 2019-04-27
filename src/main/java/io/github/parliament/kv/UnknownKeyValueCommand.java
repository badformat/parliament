package io.github.parliament.kv;

/**
 *
 * @author zy
 */
public class UnknownKeyValueCommand extends Exception {
    public UnknownKeyValueCommand(String s) {
        super(s);
    }
}