package io.github.parliament;

/**
 *
 * @author zy
 */
public class DuplicateKeyException extends RuntimeException {
    public DuplicateKeyException() {
        super("key already exists.");
    }
}