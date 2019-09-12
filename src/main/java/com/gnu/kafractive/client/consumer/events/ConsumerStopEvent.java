package com.gnu.kafractive.client.consumer.events;

import org.springframework.context.ApplicationEvent;

public class ConsumerStopEvent extends ApplicationEvent {
    public ConsumerStopEvent(Object source) {
        super(source);
    }
}
