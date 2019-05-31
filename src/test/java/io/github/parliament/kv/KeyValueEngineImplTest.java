package io.github.parliament.kv;

import lombok.Builder;
import lombok.NonNull;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zy
 */
class KeyValueEngineImplTest {
    private KeyValueEngineImpl keyValueEngine;
    private Path path = Paths.get("./test");
    private ExecutorService executorService = Executors.newFixedThreadPool(10);

    @BeforeEach
    void beforeEach() {
        keyValueEngine = KeyValueEngineImpl.builder().path(path).executorService(executorService).build();
    }

    @
}