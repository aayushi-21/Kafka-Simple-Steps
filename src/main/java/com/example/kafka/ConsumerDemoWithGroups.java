package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithGroups {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "my_fifth_application";
        String topic = "first_topic";

        //create consumer configs
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithGroups.class);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties .setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer config
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe the consumer to our topics
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true){
//            consumer.poll(100); //new in Kafka
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record: records){
                logger.info("key:"+ record.key()+"\n"+
                        "Value:"+ record.value()+"\n"+
                        "Partition:"+ record.partition()+"\n"+
                        "Offset:" + record.offset());
            }
        }
    }
}
