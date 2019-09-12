package com.gnu.kafractive.client.admin;

import com.gnu.kafractive.config.CommonProperties;
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
    private ApplicationEventPublisher applicationEventPublisher;
    private AdminUtils adminUtils;

    public AdminConnector(ApplicationEventPublisher applicationEventPublisher, AdminUtils adminUtils) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.adminUtils = adminUtils;
    }

    @ShellMethod(value="connect Brokers with admin client", key={"admin-connect"})
    public boolean connect(@ShellOption(defaultValue = "") String bootstrapServers){
        boolean userInputServers = !"".equals(bootstrapServers);
        boolean argsInputServers = !"-".equals(CommonProperties.bootstrapServers);
        if(userInputServers){
            bootstrapServers = bootstrapServers;
        } else if(argsInputServers){
            bootstrapServers = CommonProperties.bootstrapServers;
        } else {
            System.out.println("set bootstrap servers");
            return false;
        }
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            adminUtils.setClient(AdminClient.create(props));
            adminUtils.setConnection(true);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, "admin", true));
        } catch(TimeoutException e){
            adminUtils.setConnection(false);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, "admin",false));

        }
        return adminUtils.isConnection();
    }

    @ShellMethod(value="disconnect", key={"disconnect", "admin-close"})
    public void close(){
        if(adminUtils.isConnection()){
            System.out.println("disconnected");
            adminUtils.getClient().close();
            adminUtils.setConnection(false);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, "admin",false));
        } else {
            System.out.println("already disconnected");
        }
    }
}
