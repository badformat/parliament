package io.github.parliament;

public interface StateTransfer<T> {
    ReplicateStateMachine.Output transform(ReplicateStateMachine.Input input) throws Exception;
}
