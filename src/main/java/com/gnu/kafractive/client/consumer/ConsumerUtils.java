package com.gnu.kafractive.client.consumer;

import com.gnu.kafractive.client.consumer.events.ConsumerStopEvent;
import com.gnu.kafractive.config.CommonProperties;
import com.gnu.kafractive.config.ConnectEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import java.util.Properties;


@ShellComponent
public class ConsumerUtils {
    private ApplicationEventPublisher applicationEventPublisher;
    private ApplicationContext ctx;
    private boolean isConsumerSet = false;
    private ConsumerRunningThread runningThread;

    public ConsumerUtils(ApplicationEventPublisher applicationEventPublisher, ApplicationContext ctx, ConsumerRunningThread runningThread) {
        this.ctx = ctx;
        this.applicationEventPublisher = applicationEventPublisher;
        this.runningThread = runningThread;
    }

    /**
     *
     * connect brokers with consumer client
     *
     * @param topic (mandatory) topics name
     * @param clientId consumer client Id
     * @param bootstrapServers server list, it can be provided with user's input args or java -jar execute args (java -jar kafractive.jar [filename])
     * @param groupId consumer group id
     * @param duration polling interval (milliseconds)
     * @param matching if set to true, consuming all topics that contains topic name (set to false will consume extactly same name)
     * @return
     */
    @ShellMethod(value = "connect between consumer and broker", key = {"consumer-connect", "cc"})
    public boolean connect(String topic,
                           @ShellOption(defaultValue = "") String clientId,
                           @ShellOption(defaultValue = "") String bootstrapServers,
                           @ShellOption(defaultValue = "") String groupId,
                           @ShellOption(defaultValue = "1000") String duration,
                           @ShellOption(defaultValue = "false") String matching) {
        boolean userInputServers = !"".equals(bootstrapServers);
        boolean argsInputServers = !"-".equals(CommonProperties.bootstrapServers);
        if (userInputServers) {
            bootstrapServers = bootstrapServers;
        } else if (argsInputServers) {
            bootstrapServers = CommonProperties.bootstrapServers;
        } else {
            System.out.println("set bootstrap servers");
            return false;
        }

        try {
            Properties props = new Properties();
            if ("".equals(groupId)) {
                groupId = "group-" + System.currentTimeMillis();
            }
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            if (!"".equals(clientId)) {
                props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            }
            boolean flag = matching.equals("true") ? true : false;
            runningThread.init(applicationEventPublisher, topic, props, duration, flag).consume();
            return isConsumerSet = true;
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        return isConsumerSet = false;
    }

    @ShellMethod(value = "close consumer", key = {"consumer-close", "cclose"})
    @ShellMethodAvailability("availabilityConsumerClose")
    public void close() {
        System.out.println("stop consumer");
        if (CommonProperties.connectionStatus.get(CommonProperties.CONSUMER_MODE)) {
            System.out.println("disconnected");
            isConsumerSet = false;
            applicationEventPublisher.publishEvent(new ConsumerStopEvent(this));
            applicationEventPublisher.publishEvent(new ConnectEvent(this, "consumer", false));
        } else {
            System.out.println("already disconnected");
        }
    }

    private Availability availabilityConsumerStart() {
        return isConsumerSet ? Availability.available() : Availability.unavailable("you should set consumers from starting");
    }

    private Availability availabilityConsumerClose() {
        return CommonProperties.connectionStatus.get(CommonProperties.CONSUMER_MODE) ? Availability.available() : Availability.unavailable("you can't stop consumers, consumers is not currently running...");
    }
}
