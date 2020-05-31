package io.github.parliament;

public interface StateTransfer {
    ReplicateStateMachine.Output transform(ReplicateStateMachine.Input input) throws Exception;
}
