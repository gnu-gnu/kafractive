package com.gnu.kafractive.config;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.util.HashMap;
import java.util.Map;

@ShellComponent
public class CommonProperties {
    public static String bootstrapServers = "-";
    public static final String ADMIN_MODE = "admin";
    public static final String CONSUMER_MODE = "consumer";
    public static final String PRODUCER_MODE = "producer";
    public static final String JXMX_MODE = "jmx";

    public static Map<String, Boolean> connectionStatus = new HashMap<>();

    @ShellMethod(value = "show common status", key = {"show-status"})
    public void showStatus(){
        System.out.printf("Bootstrap servers - %s\n", bootstrapServers);
        System.out.println("Connection status");
        connectionStatus.entrySet().forEach(entry -> {
            System.out.printf("\t%s - %s\n", entry.getKey(), entry.getValue());
        });
    }
}
