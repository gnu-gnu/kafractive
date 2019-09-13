package com.gnu.kafractive.client.consumer;

import com.gnu.kafractive.client.consumer.events.ConsumerStopEvent;
import com.gnu.kafractive.config.ConnectEvent;
import kafka.server.ReplicaManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static com.gnu.kafractive.config.CommonProperties.CONSUMER_MODE;
import static com.gnu.kafractive.config.CommonProperties.connectionStatus;

@ShellComponent
public class ConsumerRunningThread {



    private ApplicationEventPublisher applicationEventPublisher;
    private String topicName;
    private KafkaConsumer<String, String> consumer = null;
    private Duration duration;
    private UnaryOperator<Consumer<String, String>> postJob = null;
    private boolean locking = false;

    private static Void exception(Throwable throwable) {
        if (throwable instanceof WakeupException) {
            System.out.println("Consumer successfully stopped");
        }
        return null;
    }

    public ConsumerRunningThread init(ApplicationEventPublisher applicationEventPublisher, String topicName, Properties props, String duration, boolean matching) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.topicName = topicName;
        this.duration = Duration.ofMillis(Long.valueOf(duration));
        this.consumer = new KafkaConsumer<String, String>(props);
        if (matching) {
            topicName = topicName + ".*";
        }
        consumer.subscribe(Pattern.compile(topicName));
        return this;
    }

    public void consume() {
        applicationEventPublisher.publishEvent(new ConnectEvent(this, "consumer", true));
        CompletableFuture<KafkaConsumer<String, String>> kafkaConsumerCompletableFuture = CompletableFuture.supplyAsync(this::fetch);
        if (postJob != null && locking == false) {
            locking = true;
            kafkaConsumerCompletableFuture.thenApplyAsync(postJob).thenRunAsync(this::consume).exceptionally(ConsumerRunningThread::exception).thenRun(() -> {
                postJob = null;
                locking = false;
            });
        } else {
            kafkaConsumerCompletableFuture.thenRunAsync(this::consume).exceptionally(ConsumerRunningThread::exception);
        }
    }


    private KafkaConsumer<String, String> fetch() {
        ConsumerRecords<String, String> records = consumer.poll(duration);
        records.forEach(record -> System.out.printf("\n(Consumer)[%s:P-%d:O-%d] %s", record.topic(), record.partition(), record.offset(), record.value()));
        return consumer;
    }

    @ShellMethod(value = "show subscriptions of running comsumer", key = {"show-subscription", "ss"})
    @ShellMethodAvailability(value = "checkComsumerRunningAvailability")
    public void showSubscription() {
        System.out.println();
        if (postJob == null) {
            postJob = (consumer) -> {
                consumer.subscription().forEach(topic -> {
                    System.out.printf("\tSubscription - %s\n", topic);
                });
                return consumer;
            };
        } else {
            System.out.println("registered operation exist... please wait");
        }
    }

    @ShellMethod(value = "show consumer's metrics", key = {"show-consumer-metrics", "scm"})
    @ShellMethodAvailability(value = "checkComsumerRunningAvailability")
    public void showConsumerMetrics(@ShellOption(defaultValue = "no condition") String filter) {
        System.out.println("show consumer's metric filtered with "+filter);
        if (postJob == null) {
            postJob = (consumer) -> {
                MetricName metricName = null;
                for (Map.Entry<MetricName, ? extends Metric> metricNameEntry : consumer.metrics().entrySet()) {
                    metricName = metricNameEntry.getValue().metricName();
                    boolean filterCaptured = metricName.name().contains(filter) || metricName.group().contains(filter);
                    if (filterCaptured){
                        // System.out.printf("\n%s - %s", metricName.name(), metricNameEntry.getValue().metricValue());
                        // System.out.printf("\n\t%s", metricName.tags());
                    } else if("no condition".equals(filter)){
                        // System.out.printf("\n%s - %s", metricName.name(), metricNameEntry.getValue().metricValue());
                    }
                    System.out.println(metricName.group()+" / "+metricName.name()+" / "+metricName.description());
                }
                return consumer;
            };
        } else {
            System.out.println("registered operation exist... please wait");
        }
    }

    @ShellMethodAvailability
    public Availability checkComsumerRunningAvailability() {
        return connectionStatus.get(CONSUMER_MODE) ? Availability.available() : Availability.unavailable("Consumer is not running");
    }

    @EventListener(classes = {ConsumerStopEvent.class})
    public void close() {
        consumer.wakeup();
        applicationEventPublisher.publishEvent(new ConnectEvent(this, "consumer", false));
    }
}
