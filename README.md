# Kafka Reactive Streams Java Client (Draft, WIP)

## Motivation
* It is better to understand the reactive-streams approach
* Disappointment in [reactor-kafka](https://github.com/reactor/reactor-kafka)

## Identified shortcomings of the [reactor-kafka](https://github.com/reactor/reactor-kafka)
Reactor-kafka works well in the simple scenarios that are provided in the documentation, but it does not have enough flexibility.

* I prefer an approach without reference to a specific implementation (Reactor, RxJava), as is done in [mongo-java-driver-reactivestreams](https://github.com/mongodb/mongo-java-driver-reactivestreams)
* It is not possible to process the batching, only per message
* Pausing only to all partitions of the assignment
* DefaultKafkaReceiver does not comply with backpressure

## Expectation
Configure Publisher to retrieve data from kafka:
```java
    Consumer<K, V> consumer = new KafkaConsumer<>(...)
    Publisher<ConsumerRecords<K, V>> source = ConsumerRecordsPublisher.create(consumer, Duration.ofSeconds(1));
```
Reactor using:
```java
    Flux<ConsumerRecords<K, V>> flux = Flux.from(source);
```
RxJava using:
```java
    Flowable<ConsumerRecords<K, V>> flux = Flowable.fromPublisher(source);
```

To be continued...

 


