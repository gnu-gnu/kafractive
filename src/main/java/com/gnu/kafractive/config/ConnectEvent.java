package com.gnu.kafractive.config;

import org.springframework.context.ApplicationEvent;

public class ConnectEvent extends ApplicationEvent {
    private String mode;
    private boolean connection;

    public ConnectEvent(Object source, String mode, boolean connection) {
        super(source);
        this.mode = mode;
        this.connection = connection;
    }

    public boolean isConnection() {
        return connection;
    }

    public void setConnection(boolean connection) {
        this.connection = connection;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
