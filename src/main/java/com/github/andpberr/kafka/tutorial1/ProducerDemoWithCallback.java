package com.github.andpberr.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		
		String bootstrapServers = "localhost:9092";
		String serializer = StringSerializer.class.getName();	
		
		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
				bootstrapServers);
		
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
				serializer);
		
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
				serializer);

		// create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for (int i = 0; i < 10; ++i) {
			// create a producer record
			 ProducerRecord<String, String> record =
					 new ProducerRecord<String, String>("first_topic", "hello world" + Integer.toString(i));
			
			// send data - asynchronous
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if (e == null) {
						// record was successfully sent
						logger.info("Received new metadata: \n" +
								"Topic:" + recordMetadata.topic() + "\n" +
								"Partition:" + recordMetadata.partition() + "\n" +
								"Offset:" + recordMetadata.offset() + "\n" +
								"Timestamp:" + recordMetadata.timestamp()
							);
					} else {
						logger.error("Error while producing", e);
					}
				}
			});
		}
		
		// flush data
		producer.flush();
		
		// flush and close producer
		producer.close();
	}
}

