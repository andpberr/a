package com.github.andpberr.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	public static void main(String[] args) {
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
		
		// create a producer record
		 ProducerRecord<String, String> record =
				 new ProducerRecord<String, String>("first_topic", "hello world");
		
		// send data - asynchronous
		producer.send(record);
		
		// flush data
		producer.flush();
		
		// flush and close producer
		producer.close();
	}
}

