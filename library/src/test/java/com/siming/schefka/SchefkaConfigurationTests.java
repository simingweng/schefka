package com.siming.schefka;

import com.siming.schefka.annotation.SchefkaConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SchefkaConfiguration.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(partitions = 1, controlledShutdown = true, topics = {"immediate", "delayed_5"})
public class SchefkaConfigurationTests {
    private static final Logger logger = LoggerFactory.getLogger(SchefkaConfigurationTests.class);
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private Scheduler scheduler;
    @SpyBean
    private Worker worker;

    @Test
    public void contextLoads() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        doAnswer(invocationOnMock -> {
            logger.info("received record {}", invocationOnMock.getArgument(0).toString());
            ((Acknowledgment) invocationOnMock.getArgument(1)).acknowledge();
            countDownLatch.countDown();
            return null;
        }).when(worker).onMessage(any(ConsumerRecord.class), any(Acknowledgment.class));
        registry.getListenerContainers().forEach(messageListenerContainer -> {
            try {
                ContainerTestUtils.waitForAssignment(messageListenerContainer, 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        scheduler.schedule(5, "this is a delayed user data");
        scheduler.schedule(0, "this is an immediate user date");
        countDownLatch.await(6, TimeUnit.SECONDS);
        assertThat(countDownLatch.getCount()).isZero();
    }

}
