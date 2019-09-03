package ru.deripas.reactivestreams;

import io.reactivex.processors.PublishProcessor;
import org.jctools.util.Pow2;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.deripas.reactivestreams.core.DelegateSubscriber;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class BackPressureBufferProcessorTest {

    private BackPressureBufferProcessor<String> processor;
    private Subscriber<String> subscriber;
    private Subscription subscription;
    private Runnable pauseCallback;
    private Runnable resumeCallback;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void init() {
        pauseCallback = mock(Runnable.class);
        resumeCallback = mock(Runnable.class);
        processor = BackPressureBufferProcessor.create(100, 10, pauseCallback, resumeCallback);
        processor.onSubscribe(mock(Subscription.class));

        subscriber = mock(Subscriber.class);
        processor.subscribe(new DelegateSubscriber<String>(subscriber) {
            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                super.onSubscribe(s);
            }
        });
        verify(subscriber).onSubscribe(subscription);
    }

    @Test
    public void testOnComplete() {
        processor.onComplete();
        verify(subscriber).onComplete();
    }

    @Test
    public void testOnError() {
        Exception exception = new IllegalStateException();
        processor.onError(exception);
        verify(subscriber).onError(exception);
    }

    @Test
    public void testOnNext() {
        subscription.request(1);
        processor.onNext("1");
        verify(subscriber).onNext("1");

        subscription.request(1);
        processor.onNext("2");
        verify(subscriber).onNext("2");
    }

    @Test
    public void testCallback() {
        subscription.request(1);
        processor.onNext("1");
        verify(subscriber).onNext("1");

        for (int i = 0; i < 10; i++) {
            processor.onNext("to buffer");
            verifyZeroInteractions(subscriber);
        }

        verifyZeroInteractions(pauseCallback);
        verifyZeroInteractions(resumeCallback);

        processor.onNext("buffer full");
        verify(pauseCallback).run();

        subscription.request(2);
        verify(resumeCallback).run();
    }

    @Test
    public void testFullBufferError() {
        for (int i = 0; i < Pow2.roundToPowerOfTwo(100); i++) {
            processor.onNext("to buffer");
            verifyZeroInteractions(subscriber);
        }
        processor.onNext("buffer full");
        verify(subscriber).onError(any(Exception.class));
    }
}
