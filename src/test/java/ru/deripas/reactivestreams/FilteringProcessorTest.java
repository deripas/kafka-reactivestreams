package ru.deripas.reactivestreams;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class FilteringProcessorTest {

    private FilteringProcessor<String> processor;
    private Subscriber<String> subscriber;
    private Subscription subscription;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void init() {
        processor = FilteringProcessor.create(String::isEmpty);
        subscription = mock(Subscription.class);
        processor.onSubscribe(subscription);

        subscriber = mock(Subscriber.class);
        processor.subscribe(subscriber);
        verify(subscriber).onSubscribe(any(Subscription.class));
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
        processor.onNext("1");
        verify(subscriber).onNext("1");

        processor.onNext("2");
        verify(subscriber).onNext("2");
    }

    @Test
    public void testIgnoreOnNext() {
        processor.onNext("");
        verifyNoMoreInteractions(subscriber);
        verify(subscription).request(1);
    }
}
