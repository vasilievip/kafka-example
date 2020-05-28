package com.example.kafkademo;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
@SpringBootTest(classes = {
        KafkaDemoApplication.class,
        KafkaDemoApplicationTests.Config1.class,
        KafkaDemoApplicationTests.Config2.class
})
@ActiveProfiles("test")
class KafkaDemoApplicationTests {

    @Autowired
    KafkaTemplate<String, FirstEvent> kafkaTemplate;

    @Value("${kafka-demo.topic1.name}")
    String topic1;

    @Autowired
    MessagesHolder messagesHolder;

    @Test
    void shouldSendAndReceiveKafkaEvents() throws InterruptedException {
        kafkaTemplate.send(topic1, new FirstEvent("1"));

        await().atMost(30, SECONDS).until(() -> messagesHolder.getSecondEvents().size() > 0);

        assertThat(messagesHolder.getSecondEvents().size()).isGreaterThan(0);
        assertThat(messagesHolder.getSecondEvents().get(0).getField1()).isEqualTo("1");
    }

    @Configuration(proxyBeanMethods = false)
    public static class Config1 {

        @Bean
        MessagesHolder messagesHolder() {
            return new MessagesHolder();
        }
    }

    @Configuration(proxyBeanMethods = false)
    public static class Config2 {

        @Autowired
        MessagesHolder messagesHolder;

        @KafkaListener(id = "2", topics = "${kafka-demo.topic2.name}")
        void listen(SecondEvent foo) {
            log.info("Received: " + foo);
            messagesHolder.getSecondEvents().add(foo);
        }
    }

    @NoArgsConstructor
    @Getter
    public static class MessagesHolder {
        final List<SecondEvent> secondEvents = new CopyOnWriteArrayList<>();
    }

}
