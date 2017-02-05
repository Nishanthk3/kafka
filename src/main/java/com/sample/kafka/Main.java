package com.sample.kafka;

import java.util.Date;
import java.util.concurrent.ExecutionException;

public class Main 
{
    public static void main( String[] args ) throws InterruptedException, ExecutionException
    {
        KafkaProducerServiceImpl producer = new KafkaProducerServiceImpl();
        KafkaConsumerServiceImpl consumer = new KafkaConsumerServiceImpl();
        
        consumer.consume((Message i) -> System.out.println(i.toString()));
        Message message = null;
        
        for(int i = 0; ;i++)
        {
            message = new Message();
            message.setId(i);
            message.setMessage("This is messsage : "+i);
            message.setDate(new Date());
            producer.sendMessage(true, message);
            Thread.sleep(2000); 
        }
    }
}
