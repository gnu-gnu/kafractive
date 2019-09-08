package com.gnu.kafractive.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@ShellComponent
public class AdminUtils {

    private AdminClient client;
    private boolean connection = false;

    @ShellMethod(value="get controller info", key={"show-controller"})
    public Node showControllerInfo() throws ExecutionException, InterruptedException {
        return client.describeCluster().controller().get();
    }
    @ShellMethod(value="show all nodes", key={"show-all-nodes"})
    public Collection<Node> showAllNodesList() throws ExecutionException, InterruptedException {
        return client.describeCluster().nodes().get();
    }
    @ShellMethod(value="show specific node's config", key={"show-node-config"})
    public void showNodeConfig(String brokerId, @ShellOption(defaultValue="") String filter) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        showConfigInfo(resource, filter);
    }
    @ShellMethod(value="show specific topic's config", key={"show-topic-config"})
    public void showTopicConfigInfoByName(String topicName, @ShellOption(defaultValue="",help = "if config key or value contains this filter string, it will be shown") String filter) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource((ConfigResource.Type.TOPIC), topicName);
        showConfigInfo(resource, filter);
    }
    @ShellMethod(value="show all topic's information in cluster", key={"show-all-topics"})
    public void showAllTopicsName() throws ExecutionException, InterruptedException {
        Set<String> topicNames = client.listTopics().names().get();
        TopicDescription topicDescription = null;
        for (Map.Entry<String, KafkaFuture<TopicDescription>> kafkaFutureEntry : client.describeTopics(topicNames).values().entrySet()) {
            topicDescription = kafkaFutureEntry.getValue().get();
            System.out.printf("topic name : %s\n", topicDescription.name());
            retrieveTopic(topicDescription);
        }
    }
    @ShellMethod(value="show specific topic's information", key={"show-topic"})
    public void showTopicInfoByName(String topicName) throws ExecutionException, InterruptedException {
        TopicDescription topicDescription = null;
        for (Map.Entry<String, TopicDescription> topicDescEntry : client.describeTopics(Arrays.asList(topicName)).all().get().entrySet()) {
            topicDescription = topicDescEntry.getValue();
            System.out.printf("topic name : %s\n", topicDescription.name());
            retrieveTopic(topicDescription);
        }
    }
    @ShellMethod(value="create topic by name", key={"create-topic"})
    public Void createTopic(String topicName,
                            @ShellOption(defaultValue = "-1", help = "if not set, use broker default") String partitions,
                            @ShellOption(defaultValue = "-1", help = "if not set, use broker default") String replicas)
                                            throws ExecutionException, InterruptedException, NoSuchFieldException {
        String firstNodeId = String.valueOf(showAllNodesList().stream().collect(Collectors.toList()).get(0).id());
        int partitionNum;
        short replicationNum;
        if("-1".equals(partitions)){
            partitionNum = Integer.parseInt(getBrokerMetricByMetricName(firstNodeId, "num.partitions"));
        } else {
            partitionNum = Integer.parseInt(partitions);
        }
        if("-1".equals(replicas)){
            replicationNum = (short)Integer.parseInt(getBrokerMetricByMetricName(firstNodeId, "default.replication.factor"));
        } else {
            replicationNum = (short)Integer.parseInt(replicas);
        }
        System.out.printf("trying to create topic %s with partition %d replication %d\n", topicName, partitionNum, replicationNum);
        CreateTopicsResult result = client.createTopics(Arrays.asList(new NewTopic(topicName, partitionNum, replicationNum)));
        return result.all().get();
    }
    @ShellMethod(value="delete topic by name", key={"delete-topic"})
    public Void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        System.out.printf("trying to delete topic %s\n", topicName);
        return client.deleteTopics(Arrays.asList(topicName)).all().get();
    }

    private String getBrokerMetricByMetricName(String brokerId, String configPropertyName) throws ExecutionException, InterruptedException, NoSuchFieldException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        Set<Map.Entry<ConfigResource, KafkaFuture<Config>>> entries = client.describeConfigs(Arrays.asList(resource)).values().entrySet();
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : entries) {
            return entry.getValue().get().get(configPropertyName).value();
        }
        throw new NoSuchFieldException();
    }

    private void retrieveTopic(TopicDescription topicDescription) {
            topicDescription.partitions().forEach(partition -> {
                StringBuilder isrStringBuilder = new StringBuilder();
                for (Node node : partition.isr()) {
                    isrStringBuilder.append(node.id()).append(" ");
                }
                StringBuilder replicaStringBuilder = new StringBuilder();
                for (Node node : partition.replicas()){
                    replicaStringBuilder.append(node.id()).append(" ");
                }
                System.out.printf("\tpartition %d / leader %d / replicas %s / ISR %s\n", partition.partition(),partition.leader().id(), replicaStringBuilder.toString(), isrStringBuilder.toString());
            });
    }

    private void showConfigInfo(ConfigResource resource, String filter) throws ExecutionException, InterruptedException {
        Set<Map.Entry<ConfigResource, KafkaFuture<Config>>> entries = client.describeConfigs(Arrays.asList(resource)).values().entrySet();
        String key = "";
        String value = "";
        System.out.println(filter);
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : entries) {
            for (ConfigEntry configEntry : entry.getValue().get().entries()) {
                key = Optional.ofNullable(configEntry.name()).orElse("");
                value = Optional.ofNullable(configEntry.value()).orElse("");
                if("".equals(filter) || key.contains(filter) || value.contains(filter)){
                    System.out.printf("[%s] %s = %s\n", configEntry.source().name(), key, value);
                }
            }
        }
    }

    public boolean isConnection() {
        return connection;
    }

    public void setConnection(boolean connection) {
        this.connection = connection;
    }

    public AdminClient getClient() {
        return client;
    }

    public void setClient(AdminClient client) {
        this.client = client;
    }

    @ShellMethodAvailability
    public Availability isAvailable(){
        return connection ? Availability.available() : Availability.unavailable("not connected, use 'connect'");
    }
}
