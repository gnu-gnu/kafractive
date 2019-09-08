package com.gnu.kafractive.admin;

import com.gnu.kafractive.config.ConnectEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.Properties;

@ShellComponent
public class AdminConnector {

    @Value(value = "${bootstrap.server:-}")
    private String defaultServer;

    private ApplicationEventPublisher applicationEventPublisher;
    private AdminUtils adminUtils;

    public AdminConnector(ApplicationEventPublisher applicationEventPublisher, AdminUtils adminUtils) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.adminUtils = adminUtils;
    }

    @ShellMethod(value="connect Brokers", key={"connect"})
    public boolean connect(@ShellOption(defaultValue = "") String bootstrapServers){
        boolean argsNotSet = "".equals(bootstrapServers);
        boolean envNotSet = "-".equals(defaultServer);
        if(!argsNotSet){
            bootstrapServers = bootstrapServers;
        } else if(argsNotSet && !envNotSet){
            bootstrapServers = defaultServer;
        } else if(argsNotSet && envNotSet){
            System.out.println("set bootstrap servers");
            return false;
        }
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            adminUtils.setClient(AdminClient.create(props));
            adminUtils.setConnection(true);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, true));
        } catch(TimeoutException e){
            adminUtils.setConnection(false);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, false));

        }
        return adminUtils.isConnection();
    }
    @ShellMethod(value="disconnect", key={"disconnect", "close"})
    public void close(){
        System.out.println("disconnected");
        adminUtils.getClient().close();
        adminUtils.setConnection(false);
        applicationEventPublisher.publishEvent(new ConnectEvent(this, false));

    }
}
