package com.siming.schefka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class Scheduler {
    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private KafkaTemplate kafkaTemplate;
    private ScheduledExecutorService pendingExecutor;

    @Autowired
    public Scheduler(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        pendingExecutor = Executors.newScheduledThreadPool(5);
    }

    public void schedule(int delayInSecond, Object userTask) {
        if (delayInSecond > 0) {
            kafkaTemplate.send("delayed_" + delayInSecond, delayInSecond, userTask);
        } else {
            kafkaTemplate.send("immediate", userTask);
        }

    }

    @KafkaListener(id = "schedulers", topicPattern = "delayed_\\d+", containerGroup = "schefka")
    public void onDelayedJob(ConsumerRecord<Integer, Object> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        long remainingDelay = record.key() * 1000 + record.timestamp() - System.currentTimeMillis();
        if (remainingDelay <= 0) {
            sendToWorker(record, acknowledgment);
        } else {
            logger.info("pausing topic {} partition {}", record.topic(), record.partition());
            consumer.pause(Collections.singleton(new TopicPartition(record.topic(), record.partition())));
            pendingExecutor.schedule(() -> {
                sendToWorker(record, acknowledgment);
                logger.info("resuming topic {} partition {}", record.topic(), record.partition());
                consumer.resume(Collections.singleton(new TopicPartition(record.topic(), record.partition())));
            }, remainingDelay, TimeUnit.MILLISECONDS);
        }
    }

    private void sendToWorker(ConsumerRecord<Integer, Object> record, Acknowledgment acknowledgment) {
        logger.info("publishing {} to worker", record.value());
        kafkaTemplate.send("immediate", record.value());
        acknowledgment.acknowledge();
    }
}
