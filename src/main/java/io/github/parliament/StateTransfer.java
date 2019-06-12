package io.github.parliament;

public interface StateTransfer<T> {
    Output transform(Input input) throws Exception;
}
