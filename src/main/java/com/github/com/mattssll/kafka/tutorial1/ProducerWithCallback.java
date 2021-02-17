package com.github.com.mattssll.kafka.tutorial1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
        // create Producer properties
        String bootstrapServers = "127.0.0.1:9092";
        Integer numberOfMessagesToSend = 1000;
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // kafka will always convert to bytes (0s and 1s), that's why serializer is important

        // create the producer
        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);
        for (int i=0; i<numberOfMessagesToSend; i++){

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    "first_topic", "hello world" + Integer.toString(i));
            //producer.send(record);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // callback is called everytime a message is sent or exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
