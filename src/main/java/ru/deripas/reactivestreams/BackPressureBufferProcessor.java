package ru.deripas.reactivestreams;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.SpscArrayQueue;
import ru.deripas.reactivestreams.core.BaseProcessor;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class BackPressureBufferProcessor<T> extends BaseProcessor<T, T> {

    private final Queue<T> queue;
    private final int pauseThreshold;
    private final Runnable pauseCallback;
    private final int resumeThreshold;
    private final Runnable resumeCallback;
    private final AtomicBoolean paused = new AtomicBoolean();

    public static <T> BackPressureBufferProcessor<T> create(int bufferSize, int threshold,
                                                            Runnable pauseCallback, Runnable resumeCallback) {
        return create(() -> new SpscArrayQueue<>(bufferSize), threshold, pauseCallback, threshold, resumeCallback);
    }

    public static <T> BackPressureBufferProcessor<T> create(Supplier<Queue<T>> queueSupplier, int threshold,
                                                            Runnable pauseCallback, Runnable resumeCallback) {
        return create(queueSupplier, threshold, pauseCallback, threshold, resumeCallback);
    }

    public static <T> BackPressureBufferProcessor<T> create(int bufferSize,
                                                            int pauseThreshold, Runnable pauseCallback,
                                                            int resumeThreshold, Runnable resumeCallback) {
        return create(() -> new SpscArrayQueue<>(bufferSize), pauseThreshold, pauseCallback, resumeThreshold, resumeCallback);
    }

    public static <T> BackPressureBufferProcessor<T> create(Supplier<Queue<T>> queueSupplier,
                                                            int pauseThreshold, Runnable pauseCallback,
                                                            int resumeThreshold, Runnable resumeCallback) {
        if (resumeThreshold <= 0) throw new IllegalArgumentException("must: resumeThreshold > 0");
        if (pauseThreshold <= 0) throw new IllegalArgumentException("must: pauseThreshold > 0");
        if (resumeThreshold > pauseThreshold)
            throw new IllegalArgumentException("must: pauseThreshold >= resumeCallback");

        return new BackPressureBufferProcessor<>(queueSupplier.get(), pauseThreshold, pauseCallback, resumeThreshold, resumeCallback);
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
                subscription().cancel();
                onError(new Exception("Buffer is full"));
                return;
            }
            tryPause();
        }
    }

    private void flush() {
        T item;
        while (needMore() && (item = queue.poll()) != null) {
            fireOnNext(item);
        }
        tryResume();
    }

    private void tryResume() {
        if (queue.size() < resumeThreshold && paused.compareAndSet(true, false)) {
            log.info("resume callback");
            resumeCallback.run();
        }
    }

    private void tryPause() {
        if (queue.size() > pauseThreshold && paused.compareAndSet(false, true)) {
            log.info("pause callback");
            pauseCallback.run();
        }
    }

    private void fireOnNext(T item) {
        log.info("fire onNext({})", item);
        subscriber().onNext(item);
    }
}
