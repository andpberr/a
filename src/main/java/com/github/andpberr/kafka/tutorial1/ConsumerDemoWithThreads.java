package com.github.andpberr.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {
	public static void main(String[] args) {
		new ConsumerDemoWithThreads().run();
	}
	
	private ConsumerDemoWithThreads() {
		
	}
	
	private void run() {
		String bootstrapServers = "localhost:9092";
		String deserializer = StringDeserializer.class.getName();
		String groupId = "my-sixth-application";
		String topic = "first_topic";
		Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		
		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		// create consumer

		logger.info("Creating the consumer thread");
		Runnable myConsumerRunnable = new ConsumerRunnable(latch,
				bootstrapServers,
				deserializer,
				groupId,
				topic);
		
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		// start the thread
		try {
			latch.await();
		} catch(InterruptedException e) {
			logger.error("Application got interrupted",e);
		} finally {
			logger.info("Application is closing");
		}

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() ->  {
			logger.info("Caught shutdown hook");
			((ConsumerRunnable)myConsumerRunnable).shutdown();
		}));

	}
	
	public class ConsumerRunnable implements Runnable {
		
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		
		public ConsumerRunnable(CountDownLatch latch, 
				String bootstrapServers,
				String deserializer,
				String groupId,
				String topic) {
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					bootstrapServers);

			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					deserializer);

			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					deserializer);
			
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, 
					groupId);
			
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
				"earliest");
			
			this.latch = latch;
			logger.info("Creating KafkaCons with properties:\n" + properties.toString());
			consumer = new KafkaConsumer<String, String>(properties);
			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {
			// TODO Auto-generated method stub
			try {
				while (true) {
					ConsumerRecords<String, String> records =
							consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
					
					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key: " + record.key() + ", " +
								"Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", " +
								"Offset: " + record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				consumer.close();
				// tell our main code we're done with the consumer
				latch.countDown();
			}
		}

		public void shutdown() {
			// wakeup is a special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException
			consumer.wakeup();
		}
		
	}
	
}
