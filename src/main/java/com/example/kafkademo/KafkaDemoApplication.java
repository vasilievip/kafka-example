package com.example.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-kafka
 * https://www.confluent.io/blog/apache-kafka-spring-boot-application/
 * https://www.confluent.io/blog/spring-for-apache-kafka-deep-dive-part-1-error-handling-message-conversion-transaction-support/
 */

@Slf4j
@SpringBootApplication
public class KafkaDemoApplication {

	@Autowired
	KafkaTemplate<String, SecondEvent> kafkaTemplate;

	@Value("${kafka-demo.topic2.name}")
	String topic2;

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

	@KafkaListener(id = "1", topics = "${kafka-demo.topic1.name}")
	public void listen(FirstEvent foo) {
		log.info("Received: " + foo);
		kafkaTemplate.send(topic2, new SecondEvent(foo.getField1()));
	}
}
