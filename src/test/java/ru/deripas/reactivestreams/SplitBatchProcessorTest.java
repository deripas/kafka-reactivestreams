package ru.deripas.reactivestreams;

import com.google.common.primitives.Chars;
import io.reactivex.Flowable;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import ru.deripas.reactivestreams.util.PublisherBuilder;

public class SplitBatchProcessorTest {

    @Test
    public void testRxJavaUsingUnbounded() {
        Flowable.just("aa", "", "b", "", "ccc")
                .map(s -> Chars.asList(s.toCharArray()))
                .compose(upstream -> PublisherBuilder.create(upstream)
                        .then(SplitBatchProcessor.create())
                        .build())
                .test()
                .assertSubscribed()
                .assertValues('a', 'a', 'b', 'c', 'c', 'c')
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void testRxJavaUsingRateLimit() {
        Flowable.just("aa", "", "b", "", "ccc")
                .map(s -> Chars.asList(s.toCharArray()))
                .compose(upstream -> PublisherBuilder.create(upstream)
                        .then(SplitBatchProcessor.create())
                        .build())
                .rebatchRequests(1)
                .test()
                .assertSubscribed()
                .assertValues('a', 'a', 'b', 'c', 'c', 'c')
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void testReactorUsingUnbounded() {
        StepVerifier.create(
                Flux.just("aa", "", "b", "", "ccc")
                        .map(s -> Chars.asList(s.toCharArray()))
                        .compose(flux -> PublisherBuilder.create(flux)
                                .then(SplitBatchProcessor.create())
                                .build()))
                .expectSubscription()
                .expectNext('a', 'a', 'b', 'c', 'c', 'c')
                .verifyComplete();
    }

    @Test
    public void testReactorUsingRateLimit() {
        StepVerifier.create(
                Flux.just("aa", "", "b", "", "ccc")
                        .map(s -> Chars.asList(s.toCharArray()))
                        .compose(flux -> PublisherBuilder.create(flux)
                                .then(SplitBatchProcessor.create())
                                .build()))
                .expectSubscription()
                .expectNext('a', 'a', 'b', 'c', 'c', 'c')
                .verifyComplete();
    }
}
