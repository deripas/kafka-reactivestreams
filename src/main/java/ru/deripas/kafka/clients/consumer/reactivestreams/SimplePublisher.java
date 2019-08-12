package ru.deripas.kafka.clients.consumer.reactivestreams;

import lombok.AllArgsConstructor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@AllArgsConstructor
public class SimplePublisher<T> implements Publisher<T> {

    private final AtomicReference<Subscriber<? super T>> subscriber = new AtomicReference<>();
    private final Function<Subscriber<? super T>, Subscription> onSubscribe;

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        if (!this.subscriber.compareAndSet(null, subscriber)) {
            throw new IllegalStateException("supported only single subscriber!");
        }
        subscriber.onSubscribe(onSubscribe.apply(subscriber));
    }
}
