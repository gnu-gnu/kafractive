package com.gnu.kafractive.config;

import com.gnu.kafractive.client.admin.AdminConnector;
import com.gnu.kafractive.client.admin.AdminUtils;
import com.gnu.kafractive.client.consumer.ConsumerUtils;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.context.event.EventListener;
import org.springframework.shell.jline.PromptProvider;
import org.springframework.stereotype.Component;

import static com.gnu.kafractive.config.CommonProperties.connectionStatus;

@Component
public class CustomShellProvider implements PromptProvider {

    private AdminUtils adminUtils;
    private AdminConnector adminConnector;
    private ConsumerUtils consumerUtils;

    public CustomShellProvider(AdminUtils adminUtils, AdminConnector adminConnector, ConsumerUtils consumerUtils) {
        this.adminUtils = adminUtils;
        this.adminConnector = adminConnector;
        this.consumerUtils = consumerUtils;
    }

    public static final String CONNECTED = "connected";
    public static final String DISCONNECTED = "disconnected";

    private String mode = "client";
    private String connection = DISCONNECTED;

    @Override
    public AttributedString getPrompt() {
        return new AttributedString("[kafka ]# ", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT));
    }

    @EventListener(classes = {ConnectEvent.class})
    public void handle(ConnectEvent event){
        connectionStatus.put(event.getMode(), event.isConnection());
        this.connection = event.isConnection() ? CONNECTED : DISCONNECTED;
        this.mode = !event.isConnection() ? "client" : event.getMode();
    }


}
