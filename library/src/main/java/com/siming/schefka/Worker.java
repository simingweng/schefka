package com.siming.schefka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class Worker {

    @KafkaListener(id = "workers", topicPattern = "immediate", containerGroup = "schefka")
    public void onMessage(ConsumerRecord<Object, Object> record, Acknowledgment acknowledgment) {
        acknowledgment.acknowledge();
    }
}
