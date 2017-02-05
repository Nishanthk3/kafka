package com.sample.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaProducerServiceImpl implements KafkaProducerService {

    private final static String bootstrapServers = "localhost:9092";
    private final static String byteArraySerializer = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private final static String jsonSerializer = "org.apache.kafka.connect.json.JsonSerializer";
    private final static String topicName = "test2partitions";

    private Producer producer = null;
    private ObjectMapper objectMapper = new ObjectMapper();

    public KafkaProducerServiceImpl() {
        Properties producerConfigProperties = new Properties();
        producerConfigProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfigProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, byteArraySerializer);
        producerConfigProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, jsonSerializer);
        producer = new KafkaProducer(producerConfigProperties);
    }

    @Override
    public boolean sendMessage(boolean async, Message message) throws InterruptedException, ExecutionException {
        try {
            JsonNode jsonNode = objectMapper.valueToTree(message);
            ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName, jsonNode);
            if (async) {
                producer.send(rec);
            } else {
                Future<RecordMetadata> future = producer.send(rec);
                System.out.println("Future,  offset : " + future.get().offset() +", topic : "+future.get().topic());
            }
            return true;
        } catch (Exception ex) {
            System.out.println("Exception occurred while sending the message : " + ex.getMessage());
            return false;
        }
    }

}
