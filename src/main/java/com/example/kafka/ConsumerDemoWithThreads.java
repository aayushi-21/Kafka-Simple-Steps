package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){

    }

    private void run(){

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "my_sixth_application";
        String topic = "first_topic";


        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating Consumer Threads");
        Runnable myConsumerThread = new ConsumerRunnable(countDownLatch,bootstrapServers,groupID,topic);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {

            logger.info("Caught Shutdwon hook");
            ((ConsumerRunnable) myConsumerThread).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        ));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            logger.info("Application is closing!");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(CountDownLatch latch,String bootstrapServers,String groupID,String topic){
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties .setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key:" + record.key() + "\n" +
                                "Value:" + record.value() + "\n" +
                                "Partition:" + record.partition() + "\n" +
                                "Offset:" + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received Shutdown signal!");
            }finally{
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            //the wakeup method is to interrupt consumer.poll(
            consumer.wakeup();
        }
    }
}
