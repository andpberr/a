package com.github.andpberr.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
		
		String bootstrapServers = "localhost:9092";
		String deserializer = StringDeserializer.class.getName();
		String topic = "first_topic";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServers);

		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				deserializer);

		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				deserializer);
		
		
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
				"earliest");
		
		// create consumer
		KafkaConsumer<String, String> consumer = 
				new KafkaConsumer<String, String>(properties);
		
		// assign and seek are mostly used to replay data or fetch a specific message
		
		// assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		// seek
		long offsetToReadFrom = 15L;
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		

		// subscribe consumer to our topic(s)
		consumer.subscribe(Collections.singleton(topic));
		
		int numberOfMessagesToRead = 5;
		
		// poll for new data
		for (int i = 0; i < numberOfMessagesToRead; ++i) {
			ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
			
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Key: " + record.key() + ", " +
						"Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", " +
						"Offset: " + record.offset());
			}
		}
		
		logger.info("Exiting the application");
		
	}
}
