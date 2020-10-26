package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        //create consumer configs
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer config
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offsetReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offsetReadFrom);

        //poll for new data
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while(keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record: records){
                numberOfMessagesReadSoFar+=1;
                logger.info("key:"+ record.key()+"\n"+
                        "Value:"+ record.value()+"\n"+
                        "Partition:"+ record.partition()+"\n"+
                        "Offset:" + record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
