package io.github.parliament;

/**
 *
 * @author zy
 */
public class DuplicateKeyException extends RuntimeException {
	private static final long serialVersionUID = 762236272113966586L;

	public DuplicateKeyException() {
        super("key已存在");
    }
}