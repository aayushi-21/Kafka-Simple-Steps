package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

        //Old Way
        //Step 1: create producer properties
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //Need to know how to read the data
        // properties.setProperty("key.serializer", StringSerializer.class.getName());
        // properties.setProperty("value.serializer",StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Step 2: create producer
        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);

        //create a producer record
        int i = 0;
        for(i=0;i<10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World: " + Integer.toString(i));


            //send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes every time record gets successfully entered or exception is thrown
                    if (e == null) {
                        //record is successfully sent
                        logger.info("Received new metadata \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n");
                    } else {
                        logger.error("Error producing", e);
                    }

                }
            });
        }
        producer.flush();
        producer.close();
    }
}
