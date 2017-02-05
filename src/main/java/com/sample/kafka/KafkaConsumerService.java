package com.sample.kafka;

import java.util.function.Consumer;

public interface KafkaConsumerService {

    public <T> void consume(Consumer<T> callback );
}
