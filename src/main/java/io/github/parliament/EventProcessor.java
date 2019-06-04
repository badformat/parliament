package io.github.parliament;

import java.io.IOException;

public interface EventProcessor {
    byte[] process(byte[] request);
}
