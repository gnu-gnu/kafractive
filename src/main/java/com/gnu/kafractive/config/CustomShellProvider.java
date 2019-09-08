package com.gnu.kafractive.config;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.context.event.EventListener;
import org.springframework.shell.jline.PromptProvider;
import org.springframework.stereotype.Component;

@Component
public class CustomShellProvider implements PromptProvider {

    public static final String CONNECTED = "connected";
    public static final String DISCONNECTED = "disconnected";
    private String connection = DISCONNECTED;

    @Override
    public AttributedString getPrompt() {
        return new AttributedString("[Kafka - "+ connection +" ]# ", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT));
    }

    @EventListener(classes = {ConnectEvent.class})
    public void handle(ConnectEvent event){
        this.connection = event.isConnection() ? CONNECTED : DISCONNECTED;
    }


}
