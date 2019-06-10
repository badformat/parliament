package io.github.parliament;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * 并发接受多个事件请求，为每个请求递增分配唯一共识实例编号及客户端标识。
 * 使用共识服务，对事件并发执行共识过程。
 * 追踪共识结果，并使用相应状态处理器按编号从低到高依次处理，并通知等待者执行结果。
 * 每个共识实例必须执行。且只能执行一次。
 * 每个状态实例被执行后，保存当前执行进度。
 * 应用非正常中断时（如断电，进程被杀死），相关进度数据需使用写前日志保证完整（如执行进度编号不能只写了一半到持久化存储中），
 * 并在下次运行时恢复。
 * 每个状态实例执行过程的完整性由处理对象保证。
 * <p>
 *
 * @author zy
 */
public class ReplicateStateMachine implements Runnable {
    static final byte[] RSM_DONE_REDO = "rsm_done_redo".getBytes();
    static final byte[] RSM_DONE = "rsm_done".getBytes();

    private static final Logger logger = LoggerFactory.getLogger(ReplicateStateMachine.class);
    @Setter(AccessLevel.PACKAGE)
    private EventProcessor eventProcessor;
    @Getter(AccessLevel.PACKAGE)
    private Persistence persistence;
    private Sequence<Integer> sequence;
    private Coordinator coordinator;
    private volatile Integer done = -1;
    private volatile Integer max = -1;
    private ConcurrentMap<Integer, CompletableFuture<State>> futures = new MapMaker().weakValues().makeMap();
    private volatile int forgetThreshold = 0;

    @Builder
    private ReplicateStateMachine(@NonNull Persistence persistence,
                                  @NonNull Sequence<Integer> sequence,
                                  @NonNull Coordinator coordinator) {
        this.persistence = persistence;
        this.sequence = sequence;
        this.coordinator = coordinator;
    }

    public void start(EventProcessor processor, Executor executor) throws IOException {
        this.eventProcessor = processor;
        Integer d = getRedoLog();
        if (d != null) {
            this.done = d;
        } else {
            byte[] bytes = persistence.get(RSM_DONE);
            if (bytes == null) {
                this.done = -1;
            } else {
                this.done = ByteBuffer.wrap(bytes).getInt();
            }
        }
        sequence.set(this.done + 1);
        executor.execute(this);
    }

    /**
     * 提交事件，返回分配给事件提交者的id和tag。
     *
     * @param content
     * @return event
     */
    public State state(byte[] content) throws DuplicateKeyException {
        syncMaxAndSequence();
        State state = State.builder()
                .id(next())
                .uuid(uuid())
                .content(content)
                .build();
        return state;
    }

    public CompletableFuture<State> submit(State state) throws IOException, ExecutionException {
        Preconditions.checkState(!futures.containsKey(state.getId()),
                "instance id " + state.getId() + " already exists.");
        CompletableFuture<State> future = futures.computeIfAbsent(state.getId(), (k) -> new CompletableFuture<>());
        coordinator.coordinate(state.getId(), State.serialize(state));
        return future;
    }

    @Override
    public void run() {
        for (; ; ) {
            try {
                follow();
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
    }

    void follow() throws IOException {
        syncMaxAndSequence();
        int id = done() + 1;
        Future<byte[]> instance = null;
        State state = null;
        try {
            instance = coordinator.instance(id);
            state = State.deserialize(instance.get());
        } catch (ExecutionException | InterruptedException | IOException e) {
            logger.error("Exception in coordinator.instance({})", id, e);
            return;
        } catch (ClassNotFoundException e) {
            logger.error("deserialize RSM state（id: {}) failed.Can not continue.Server exit.", id, e);
            System.exit(-1);
        }
        try {
            writeRedoLog(done());
            eventProcessor.process(state);
            if (state.isProcessed()) {
                CompletableFuture<State> future = futures.get(state.getId());
                if (future != null) {
                    future.complete(state);
                }
            } else {
                return;
            }

            done(id);

            forgetThreshold++;
            if (forgetThreshold > 100) {
                forgetThreshold = 0;
                coordinator.forget(done());
            }
        } finally {
            removeRedoLog();
        }
    }

    public int max() {
        return max;
    }

    private void max(int m) {
        max = m;
    }

    public int done() {
        return done;
    }

    private void done(int d) throws IOException {
        persistence.put(RSM_DONE, ByteBuffer.allocate(4).putInt(d).array());
        done = d;
    }

    public void forget(int before) throws IOException {
        Preconditions.checkState(before <= done());
        coordinator.forget(before);
    }

    private byte[] uuid() {
        return UUID.randomUUID().toString().getBytes();
    }

    private int next() {
        return sequence.next();
    }

    private Integer getRedoLog() throws IOException {
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

    private void removeRedoLog() throws IOException {
        persistence.remove(RSM_DONE_REDO);
    }

    private void writeRedoLog(int id) throws IOException {
        persistence.put(RSM_DONE_REDO, ByteBuffer.allocate(4).putInt(id).array());
    }

    private void syncMaxAndSequence() {
        max(coordinator.max());
        if (max() > sequence.current()) {
            sequence.set(max);
        }
    }
}
