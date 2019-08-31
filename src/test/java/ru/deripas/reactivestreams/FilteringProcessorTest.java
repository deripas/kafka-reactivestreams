package ru.deripas.reactivestreams;

import io.reactivex.Flowable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import ru.deripas.reactivestreams.util.PublisherBuilder;

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

    @Test
    public void testRxJavaUsing() {
        Flowable.just("1", "", "2", "", "3")
                .compose(upstream -> PublisherBuilder.create(upstream)
                        .then(FilteringProcessor.create(String::isEmpty))
                        .build())
                .test()
                .assertSubscribed()
                .assertValues("1", "2", "3")
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void testReactorUsing() {
        StepVerifier.create(
                Flux.just("1", "", "2", "", "3")
                        .compose(flux -> PublisherBuilder.create(flux)
                                .then(FilteringProcessor.create(String::isEmpty))
                                .build()))
                .expectSubscription()
                .expectNext("1", "2", "3")
                .verifyComplete();
    }
}
