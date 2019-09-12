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

import static com.gnu.kafractive.config.CommonProperties.ADMIN_MODE;
import static com.gnu.kafractive.config.CommonProperties.connectionStatus;

@ShellComponent
public class AdminConnector {
    private ApplicationEventPublisher applicationEventPublisher;
    private AdminUtils adminUtils;

    public AdminConnector(ApplicationEventPublisher applicationEventPublisher, AdminUtils adminUtils) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.adminUtils = adminUtils;
    }

    /**
     *
     * connect brokers with AdminClient
     *
     * @param bootstrapServers server list, it can be provided with user's input args or java -jar execute args (java -jar kafractive.jar [filename])
     * @return true, if success.
     */
    @ShellMethod(value="connect Brokers with admin client", key={"admin-connect", "ac"})
    public boolean connect(@ShellOption(defaultValue = "") String bootstrapServers){
        if(connectionStatus.get(ADMIN_MODE)){
            System.out.println("AdminClient connection is currently established, disconnect first");
            return false;
        }

        boolean userInputServers = !"".equals(bootstrapServers);
        boolean argsInputServers = !"-".equals(CommonProperties.bootstrapServers);
        if(userInputServers){
            // PASS
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
            connectionStatus.put(ADMIN_MODE,true);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, ADMIN_MODE, true));
        } catch(TimeoutException e){
            connectionStatus.put(ADMIN_MODE,false);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, ADMIN_MODE,false));

        }
        return connectionStatus.get(ADMIN_MODE);
    }

    @ShellMethod(value="disconnect", key={"admin-close", "aclose"})
    public void close(){
        if(connectionStatus.get(ADMIN_MODE)){
            System.out.println("disconnected");
            adminUtils.getClient().close();
            connectionStatus.put(ADMIN_MODE, false);
            applicationEventPublisher.publishEvent(new ConnectEvent(this, ADMIN_MODE,false));
        } else {
            System.out.println("already disconnected");
        }
    }
}
