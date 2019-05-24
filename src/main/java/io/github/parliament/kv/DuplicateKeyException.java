package io.github.parliament.kv;

/**
 *
 * @author zy
 */
class DuplicateKeyException extends Exception {
    DuplicateKeyException() {
        super("key already exists.");
    }
}