package com.github.andpberr.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
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
			
			String topic = "first_topic";
			String value = "hello world" + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			
			ProducerRecord<String, String> record =
					 new ProducerRecord<String, String>(topic, key, value);
			
			logger.info("Key: " + key); // log the key
			//id 0 is going to partition 1
			//id 1 is going to partition 0
			//id 2 is going to partition 2
			//id 3 is going to partition 0
			//id 4 is going to partition 2
			//id 5 is going to partition 2
			//id 6 is going to partition 0
			//id 7 is going to partition 2
			//id 8 is going to partition 1
			//id 9 is going to partition 2
			
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
			}).get(); // block .send() to make it synchronous... do not do in practice
		}
		
		// flush data
		producer.flush();
		
		// flush and close producer
		producer.close();
	}
}

