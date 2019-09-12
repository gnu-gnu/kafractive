package com.gnu.kafractive.client.consumer;

import com.gnu.kafractive.config.ConnectEvent;
import com.gnu.kafractive.client.consumer.events.ConsumerStopEvent;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

@Component
public class ConsumerRunningThread {

    private ApplicationEventPublisher applicationEventPublisher;
    private String topicName;
    private KafkaConsumer<String, String> consumer = null;
    private Duration duration;

    private static Void exception(Throwable throwable) {
        if (throwable instanceof WakeupException) {
            System.out.println("Consumer successfully stopped");
        }
        return null;
    }

    public ConsumerRunningThread init(ApplicationEventPublisher applicationEventPublisher, String topicName, Properties props, String duration, boolean matching){
        this.applicationEventPublisher = applicationEventPublisher;
        this.topicName = topicName;
        this.duration = Duration.ofMillis(Long.valueOf(duration));
        this.consumer = new KafkaConsumer<String, String>(props);
        consumer = new KafkaConsumer<String, String>(props);
        if(matching){
            topicName = topicName + ".*";
        }
        consumer.subscribe(Pattern.compile(topicName));
        return this;
    }

    public void consume(){
        applicationEventPublisher.publishEvent(new ConnectEvent(this, "consumer", true));
        CompletableFuture.runAsync(this::fetch).thenAcceptAsync(this::accept).exceptionally(ConsumerRunningThread::exception);
    }

    private void fetch() {
        ConsumerRecords<String, String> records = consumer.poll(duration);
        records.forEach(record -> System.out.printf("\n(Consumer)[%s:P-%d:O-%d] %s", record.topic(), record.partition(), record.offset(), record.value()));
    }

    private void accept(Void asyncVoid) {
        consume();
    }

    @EventListener(classes = {ConsumerStopEvent.class})
    public void close(){
        consumer.wakeup();
        applicationEventPublisher.publishEvent(new ConnectEvent(this, "consumer", false));
    }
}
