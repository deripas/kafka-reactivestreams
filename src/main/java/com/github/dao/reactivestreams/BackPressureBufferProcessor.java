package com.github.dao.reactivestreams;

import com.github.dao.reactivestreams.core.BaseProcessor;
import com.github.dao.reactivestreams.util.EmptyRunnable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.SpscArrayQueue;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class BackPressureBufferProcessor<T> extends BaseProcessor<T, T> {

    private final Queue<T> queue;
    private final int pauseThreshold;
    private final Runnable pauseCallback;
    private final int resumeThreshold;
    private final Runnable resumeCallback;
    private final AtomicBoolean paused = new AtomicBoolean();

    public static <T> BackPressureBufferProcessor<T> create(int capacity) {
        return create(new SpscArrayQueue<>(capacity),
                Integer.MAX_VALUE, EmptyRunnable.INSTANCE, EmptyRunnable.INSTANCE);
    }

    public static <T> BackPressureBufferProcessor<T> create(Queue<T> queue, int threshold,
                                                            Runnable pauseCallback, Runnable resumeCallback) {
        return create(queue, threshold, pauseCallback, threshold, resumeCallback);
    }

    public static <T> BackPressureBufferProcessor<T> create(Queue<T> queue,
                                                            int pauseThreshold, Runnable pauseCallback,
                                                            int resumeThreshold, Runnable resumeCallback) {
        if (resumeThreshold <= 0) {
            throw new IllegalArgumentException("resumeThreshold < 0");
        }
        if (pauseThreshold <= 0) {
            throw new IllegalArgumentException("pauseThreshold < 0");
        }
        if (resumeThreshold > pauseThreshold) {
            throw new IllegalArgumentException("pauseThreshold < resumeCallback");
        }
        return new BackPressureBufferProcessor<>(queue, pauseThreshold, pauseCallback, resumeThreshold, resumeCallback);
    }

    @Override
    protected void doOnRequest() {
        flush();
        super.doOnRequest();
    }

    @Override
    public void onNext(T item) {
        log.info("onNext({})", item);
        flush();
        if (needMore()) {
            fireOnNext(item);
        } else {
            if (!queue.offer(item)) {
                onError(new Exception("Buffer is full"));
                return;
            }
            pauseIfNeed();
        }
    }

    private void flush() {
        T item;
        while (needMore() && (item = queue.poll()) != null) {
            fireOnNext(item);
        }
        resumeIfNeed();
    }

    private void resumeIfNeed() {
        if (queue.size() < resumeThreshold && paused.compareAndSet(true, false)) {
            log.info("resume callback");
            tryRun(resumeCallback);
        }
    }

    private void pauseIfNeed() {
        if (queue.size() > pauseThreshold && paused.compareAndSet(false, true)) {
            log.info("pause callback");
            tryRun(pauseCallback);
        }
    }

    private void fireOnNext(T item) {
        log.info("fire onNext({})", item);
        subscriber().onNext(item);
    }

    private void tryRun(Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            onError(e);
        }
    }
}
