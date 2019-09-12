package com.gnu.kafractive.config;

import com.gnu.kafractive.client.admin.AdminConnector;
import com.gnu.kafractive.client.admin.AdminUtils;
import com.gnu.kafractive.client.consumer.ConsumerConnector;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.context.event.EventListener;
import org.springframework.shell.jline.PromptProvider;
import org.springframework.stereotype.Component;

@Component
public class CustomShellProvider implements PromptProvider {

    private AdminUtils adminUtils;
    private AdminConnector adminConnector;
    private ConsumerConnector consumerConnector;

    public CustomShellProvider(AdminUtils adminUtils, AdminConnector adminConnector, ConsumerConnector consumerConnector) {

        this.adminUtils = adminUtils;
        this.adminConnector = adminConnector;
        this.consumerConnector = consumerConnector;
    }

    public static final String CONNECTED = "connected";
    public static final String DISCONNECTED = "disconnected";
    private String mode = "Client";
    private String connection = DISCONNECTED;
    private String adminConnection = DISCONNECTED;
    private String consumerConnection = DISCONNECTED;
    private String producerConnection = DISCONNECTED;


    @Override
    public AttributedString getPrompt() {
        return new AttributedString("["+mode+" - "+ connection +" ]# ", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT));
    }

    @EventListener(classes = {ConnectEvent.class})
    public void handle(ConnectEvent event){
        if(this.mode.equals("admin") && adminUtils.isConnection()){
            adminConnector.close();
        } else if(this.mode.equals("consumer")){

        } else if(this.mode.equals("producer")){

        } else if(this.mode.equals("client")){

        }

        this.connection = event.isConnection() ? CONNECTED : DISCONNECTED;
        this.mode = !event.isConnection() ? "client" : event.getMode();
    }


}
