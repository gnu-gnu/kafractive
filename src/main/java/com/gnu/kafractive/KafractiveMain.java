package com.gnu.kafractive;

import com.gnu.kafractive.config.CommonProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@SpringBootApplication
public class KafractiveMain {
    private static final Logger LOG = LoggerFactory.getLogger(KafractiveMain.class);
    public static void main(String[] args) throws IOException {
        serverListFromArgs(args);
        initialConnectionStatus();
        SpringApplication.run(KafractiveMain.class, args);
    }

    private static void serverListFromArgs(String[] args) throws IOException {
        if (args.length > 0){
            List<String> list = new ArrayList<>();
            try(BufferedReader br = new BufferedReader(new FileReader(new File(args[0])))){
                String line = "";
                while((line = br.readLine()) != null){
                    if(!line.startsWith("#")){
                        list.add(line);
                    }
                }
                String bootstrap = String.join(",", list);
                CommonProperties.bootstrapServers = bootstrap;
                LOG.info("using pre-defined bootstrap server list : {}", CommonProperties.bootstrapServers);
            }
        }
    }

    private static void initialConnectionStatus(){
        Map<String, Boolean> status = CommonProperties.connectionStatus;
        status.put(CommonProperties.ADMIN_MODE, false);
        status.put(CommonProperties.PRODUCER_MODE, false);
        status.put(CommonProperties.CONSUMER_MODE, false);
        status.put(CommonProperties.JXMX_MODE, false);
    }

}
