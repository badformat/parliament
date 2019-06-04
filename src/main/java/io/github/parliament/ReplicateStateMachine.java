package io.github.parliament;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
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
 * TODO 移到coordinator部分
 * 删除办法：
 * <ul>
 * <li>咨询所有rsm已处理事件的最大编号</li>
 * <li>从以上编号中取最小</li>
 * <li>删除该最小值之前的事件</li>
 * </ul>
 *
 * @author zy
 */
@Builder
public class ReplicateStateMachine implements Runnable {
    static final byte[] RSM_DONE_REDO = "rsm_done_redo".getBytes();
    static final byte[] RSM_DONE = "rsm_done".getBytes();

    private static final Logger logger = LoggerFactory.getLogger(ReplicateStateMachine.class);
    @NonNull
    private EventProcessor eventProcessor;
    @NonNull
    @Getter(AccessLevel.PACKAGE)
    private Persistence persistence;
    @NonNull
    private Sequence<Integer> sequence;
    @NonNull
    private Coordinator coordinator;
    @Builder.Default
    private volatile Integer done = -1;
    @Builder.Default
    private volatile Integer max = -1;

    @Builder.Default
    private ConcurrentMap<Integer, CompletableFuture<State>> futures = new MapMaker().weakValues().makeMap();

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

    public CompletableFuture<State> submit(State state) throws IOException {
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
            } catch (IOException | ExecutionException | ClassNotFoundException e) {
                logger.error("Failed in rsm follow().", e);
            } catch (InterruptedException e) {
                logger.info("RSM Thread is interrupted. exit.", e);
                return;
            }
        }
    }

    void follow() throws ExecutionException, InterruptedException, IOException, ClassNotFoundException {
        syncMaxAndSequence();
        int id = done() + 1;
        Future<byte[]> instance = coordinator.instance(id);
        State state = State.deserialize(instance.get());
        try {
            writeRedoLog(done());
            byte[] output = eventProcessor.process(state.getContent());
            state.setProcessed(true);
            state.setOutput(output);
            CompletableFuture<State> future = futures.get(state.getId());
            if (future != null) {
                future.complete(state);
            }

            done(id);
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

    public void forget(int before) {
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
