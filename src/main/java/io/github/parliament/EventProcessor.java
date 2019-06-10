package io.github.parliament;

import java.io.IOException;

public interface EventProcessor {
    void process(State state) throws IOException;
}
