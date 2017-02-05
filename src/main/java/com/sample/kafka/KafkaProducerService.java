package com.sample.kafka;

import java.util.concurrent.ExecutionException;

public interface KafkaProducerService {
    
    public boolean sendMessage(boolean async, Message message) throws InterruptedException, ExecutionException;
}
