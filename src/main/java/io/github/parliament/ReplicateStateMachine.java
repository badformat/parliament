package io.github.parliament;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * 并发接受多个事件请求，为每个请求递增分配唯一共识实例编号及客户端标识。 使用共识服务，对事件并发执行共识过程。
 * 追踪共识结果，并使用相应状态处理器按编号从低到高依次处理，并通知等待者执行结果。 每个共识实例必须执行。且只能执行一次。
 * 每个状态实例被执行后，保存当前执行进度。
 * 应用非正常中断时（如断电，进程被杀死），相关进度数据需使用写前日志保证完整（如执行进度编号不能只写了一半到持久化存储中）， 并在下次运行时恢复。
 * 每个状态实例执行过程的完整性由处理对象保证。
 * <p>
 *
 * @author zy
 */
public class ReplicateStateMachine {
    static final byte[] RSM_DONE_REDO = "rsm_done_redo".getBytes();
    static final byte[] RSM_DONE = "rsm_done".getBytes();

    private static final Logger logger = LoggerFactory.getLogger(ReplicateStateMachine.class);
    @Getter(AccessLevel.PACKAGE)
    private final LoadingCache<Integer, CompletableFuture<Output>> transforms = CacheBuilder
            .newBuilder().weakValues().build(new CacheLoader<>() {
                @Override
                public CompletableFuture<Output> load(Integer key) {
                    return new CompletableFuture<>();
                }
            });
    @Setter(AccessLevel.PACKAGE)
    private StateTransfer<String> stateTransfer;
    @Getter(AccessLevel.PACKAGE)
    private Persistence persistence;
    private Sequence<Integer> sequence;
    @Getter(AccessLevel.PACKAGE)
    private Coordinator coordinator;
    private AtomicInteger done = new AtomicInteger(-1);
    private AtomicInteger max = new AtomicInteger(-1);
    private AtomicInteger threshold = new AtomicInteger(0);
    private volatile boolean stop = false;

    @Builder
    private ReplicateStateMachine(@NonNull Persistence persistence,
            @NonNull Sequence<Integer> sequence, @NonNull Coordinator coordinator) {
        this.persistence = persistence;
        this.sequence = sequence;
        this.coordinator = coordinator;
    }

    public void start(StateTransfer<String> transfer, Executor executor)
            throws IOException, ExecutionException {
        this.stateTransfer = transfer;
        Integer d = getRedoLog();
        if (d != null) {
            this.done.set(d);
        } else {
            byte[] bytes = persistence.get(RSM_DONE);
            if (bytes == null) {
                this.done.set(-1);
            } else {
                this.done.set(ByteBuffer.wrap(bytes).getInt());
            }
        }
        sequence.set(this.done.get() + 1);
        stop = false;
        executor.execute(() -> {
            for (;;) {
                if (stop) {
                    return;
                }
                try {
                    apply();
                } catch (IOException e) {
                    logger.warn("IOException in RSM Thread.", e);
                } catch (Exception e) {
                    logger.error("Unknown exception in RSM Thread.", e);
                }
                if (Thread.currentThread().isInterrupted()) {
                    logger.info("RSM Thread is interrupted.exit.");
                    return;
                }
            }
        });
    }

    void stop() {
        this.stop = true;
    }

    /**
     * 提交事件，返回分配给事件提交者的id和tag。
     *
     * @param content 内容
     * @return event
     */
    public Input newState(byte[] content) throws DuplicateKeyException {
        return Input.builder().id(next()).uuid(uuid()).content(content).build();
    }

    public CompletableFuture<Output> submit(Input input) throws IOException, ExecutionException {
        Preconditions.checkState(input.getId() <= sequence.current(), "Instance id: "
                + input.getId() + " > sequence current value: " + sequence.current());
        coordinator.coordinate(input.getId(), Input.serialize(input));
        return transforms.get(input.getId());
    }

    void apply() throws IOException, ExecutionException {
        int id = done.get() + 1;
        Future<byte[]> inputFuture = null;
        Input input = null;
        CompletableFuture<Output> transform = null;
        try {
            inputFuture = coordinator.instance(id);
            input = Input.deserialize(inputFuture.get(100, TimeUnit.MILLISECONDS));
            transform = transforms.get(input.getId());
        } catch (ExecutionException | InterruptedException | IOException e) {
            logger.error("Exception in coordinator.instance({})", id, e);
            return;
        } catch (TimeoutException e) {
            keepUp();
            return;
        } catch (ClassNotFoundException e) {
            logger.error("deserialize RSM input（id: {}) failed.Can not continue.Server exit.", id,
                    e);
            System.exit(-1);
        }

        try {
            writeRedoLog(done());
            try {
                Output output = stateTransfer.transform(input);
                transform.complete(output);
            } catch (Exception e) {
                logger.error("Exception thrown by eventProcess.transform for instance {}",
                        input.getId(), e);
                return;
            }
            done(id);
            syncMaxAndSequence();
            forget();
        } finally {
            removeRedoLog();
        }
    }

    private void forget() throws IOException, ExecutionException {
        if (threshold.incrementAndGet() > 100) {
            threshold.set(0);
            coordinator.forget(done());
        }
    }

    private void keepUp() throws IOException, ExecutionException {
        int begin = done() + 1;
        Preconditions.checkState(begin >= 0);
        int end = coordinator.max();
        while (begin <= end) {
            coordinator.learn(begin);
            begin++;
        }
    }

    public int max() {
        return max.get();
    }

    private void max(int m) {
        max.set(m);
    }

    public int done() {
        return done.get();
    }

    private void done(int d) throws IOException, ExecutionException {
        persistence.put(RSM_DONE, ByteBuffer.allocate(4).putInt(d).array());
        done.set(d);
    }

    public void forget(int before) throws IOException, ExecutionException {
        Preconditions.checkState(before <= done());
        coordinator.forget(before);
    }

    private byte[] uuid() {
        return UUID.randomUUID().toString().getBytes();
    }

    private synchronized int next() {
        return sequence.next();
    }

    int current() {
        return sequence.current();
    }

    private Integer getRedoLog() throws IOException, ExecutionException {
        try {
            byte[] bytes = persistence.get(RSM_DONE_REDO);
            if (bytes == null) {
                return null;
            }
            return ByteBuffer.wrap(bytes).getInt();
        } catch (BufferUnderflowException | NullPointerException e) {
            logger.warn("invalid redo log.", e);
        }
        return null;
    }

    private void removeRedoLog() throws IOException, ExecutionException {
        persistence.del(RSM_DONE_REDO);
    }

    private void writeRedoLog(int id) throws IOException, ExecutionException {
        persistence.put(RSM_DONE_REDO, ByteBuffer.allocate(4).putInt(id).array());
    }

    private synchronized void syncMaxAndSequence() {
        max(coordinator.max());
        if (max() >= sequence.current()) {
            logger.debug("update sequence value to:{}", max() + 1);
            sequence.set(max() + 1);
        }
    }
}
