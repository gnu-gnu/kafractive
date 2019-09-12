package com.gnu.kafractive.client.consumer;

import com.gnu.kafractive.config.CommonProperties;
import com.gnu.kafractive.config.ConnectEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.gnu.kafractive.config.CommonProperties.bootstrapServers;


@ShellComponent
public class ConsumerConnector {
    private ApplicationEventPublisher applicationEventPublisher;

    private KafkaConsumer<String, String> consumer = null;

    private boolean isConsumerSet = false;
    private boolean isConsumerStarted = false;
    private Thread consumerThread = null;

    public ConsumerConnector(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @ShellMethod(value = "connect consumer and broker", key = {"consumer-connect"})
    public boolean connect(String topicName, @ShellOption(defaultValue = "") String clientId, @ShellOption(defaultValue = "") String bootstrapServers, @ShellOption(defaultValue = "") String groupId) {
        boolean userInputServers = !"".equals(bootstrapServers);
        boolean argsInputServers = !"-".equals(CommonProperties.bootstrapServers);
        if(userInputServers){
            bootstrapServers = bootstrapServers;
        } else if(argsInputServers){
            bootstrapServers = bootstrapServers;
        } else {
            System.out.println("set bootstrap servers");
            return false;
        }

        try {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            if(!"".equals(groupId)){
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            }
            if(!"".equals(clientId)){
                props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            }
            consumer = new KafkaConsumer<String, String>(props);
            Pattern topicPattern = Pattern.compile(topicName);
            consumer.subscribe(topicPattern);
            return isConsumerSet = true;
        } catch(TimeoutException e){
            e.printStackTrace();
        }
        return isConsumerSet = false;
    }
    @ShellMethod(value = "consumer start", key={"consumer-start"})
    @ShellMethodAvailability("availabilityConsumerStart")
    public void consumerStart(long duration) {
        consumerThread = new Thread(() -> {
            applicationEventPublisher.publishEvent(new ConnectEvent(this, "consumer",true));
            Duration pollDuration = Duration.ofMillis(duration);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(pollDuration);
                records.forEach(record -> System.out.printf("(Consumer)[%s:%d:%d] %s\n", record.topic(), record.partition(), record.offset(), record.value()));
            }
        });
        consumerThread.start();
    }

    @ShellMethod(value = "close consumer", key={"consumer-close"})
    @ShellMethodAvailability("availabilityConsumerClose")
    public void close() {
        System.out.println("stop consumer");
        if(isConsumerSet && isConsumerStarted){
            System.out.println("disconnected");
            consumer.unsubscribe();
            consumer.wakeup();
            consumer.close();
            consumerThread.interrupt();
            isConsumerSet = false;
            isConsumerStarted = false;
            applicationEventPublisher.publishEvent(new ConnectEvent(this, "consumer",false));
        } else {
            System.out.println("already disconnected");
        }

    }

    private Availability availabilityConsumerStart(){
        return isConsumerSet ? Availability.available() : Availability.unavailable("you should set consumers from starting");
    }

    private Availability availabilityConsumerClose(){
        return isConsumerStarted ? Availability.available() : Availability.unavailable("you can't stop consumers, consumers is not currently running...");
    }
}
