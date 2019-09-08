package com.gnu.kafractive.config;

import org.springframework.context.ApplicationEvent;

public class ConnectEvent extends ApplicationEvent {
    private boolean connection;

    public ConnectEvent(Object source, boolean connection) {
        super(source);
        this.connection = connection;
    }

    public boolean isConnection() {
        return connection;
    }

    public void setConnection(boolean connection) {
        this.connection = connection;
    }
}
