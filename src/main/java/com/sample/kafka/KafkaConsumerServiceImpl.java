package com.sample.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaConsumerServiceImpl implements KafkaConsumerService {

    private final static String bootstrapServers = "localhost:9092";
    private final static String byteArrayDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    private final static String jsonDeserializer = "org.apache.kafka.connect.json.JsonDeserializer";
    private final static String topicName = "test2partitions";
    private final static String groupId = "groupId";

    private KafkaConsumer<String, JsonNode> consumer = null;
    private ObjectMapper objectMapper = new ObjectMapper();

    public KafkaConsumerServiceImpl() {
        Properties consumerConfigProperties = new Properties();
        consumerConfigProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfigProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, byteArrayDeserializer);
        consumerConfigProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, jsonDeserializer);
        consumerConfigProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<String, JsonNode>(consumerConfigProperties);
        consumer.subscribe(Arrays.asList(topicName));
    }

    @Override
    public <T> void consume(Consumer<T> callback) {
        Thread thread = new Thread(() -> {
            while (!Thread.interrupted()) {
                try {
                    ConsumerRecords<String, JsonNode> records = consumer.poll(100);
                    for (ConsumerRecord<String, JsonNode> record : records) {
                        JsonNode jsonNode = record.value();
                        callback.accept((T) objectMapper.treeToValue(jsonNode, Message.class));
                    }
                } catch (Exception ex) {
                    System.out.println("Exception caught " + ex.getMessage());
                }
                if (Thread.interrupted()) {
                    consumer.close();
                    System.out.println("After closing consumer");
                }
            }
        });

        thread.start();
        
    }

}
