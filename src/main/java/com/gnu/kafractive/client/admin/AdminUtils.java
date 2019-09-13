package com.gnu.kafractive.client.admin;

import kafka.server.ReplicaManager;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.gnu.kafractive.config.CommonProperties.ADMIN_MODE;
import static com.gnu.kafractive.config.CommonProperties.connectionStatus;

@ShellComponent
public class AdminUtils {

    private AdminClient client;

    /**
     *
     * Show controller information
     * Controller is a node responsible for managing partitions and replica's status.
     *
     * @return Controller node (host, id, rack)
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="get controller info", key={"show-controller", "sc"})
    public Node showControllerInfo() throws ExecutionException, InterruptedException {
        return client.describeCluster().controller().get();
    }

    /**
     *
     * Show all nodes list
     *
     * @return Collection of nodes
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="show all nodes", key={"show-all-nodes", "san"})
    public Collection<Node> showAllNodesList() throws ExecutionException, InterruptedException {
        return client.describeCluster().nodes().get();
    }

    /**
     *
     * show specific broker's whole information
     * [STATIC_BROKER_CONFIG] is set in your configuration file.
     * [DEFAULT_CONFIG] is kafka's default value.
     *
     * @param brokerId broker'id. it can be specified in server.properties or like files.
     * @param filter filtering character, if set, results only contain this string will be shown.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="show specific node's config", key={"show-node-config", "snc"})
    public void showNodeConfig(String brokerId, @ShellOption(defaultValue="") String filter) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        showConfigInfo(resource, filter);
    }

    /**
     *
     *  show specific topic's whole information
     * [STATIC_BROKER_CONFIG] is set in your configuration file.
     * [DEFAULT_CONFIG] is kafka's default value.
     *
     * @param topicName topic's name
     * @param filter filtering character, if set, results only contain this string will be shown.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="show specific topic's config", key={"show-topic-config", "stc"})
    public void showTopicConfigInfoByName(String topicName, @ShellOption(defaultValue="",help = "if config key or value contains this filter string, it will be shown") String filter) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource((ConfigResource.Type.TOPIC), topicName);
        showConfigInfo(resource, filter);
    }

    /**
     *
     * show all topics information (name, partitons, replications, partition leader, current ISR(in-sync replicas)
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="show all topic's information in cluster", key={"show-all-topics", "sat"})
    public void showAllTopicsName() throws ExecutionException, InterruptedException {
        Set<String> topicNames = client.listTopics().names().get();
        TopicDescription topicDescription = null;
        for (Map.Entry<String, KafkaFuture<TopicDescription>> kafkaFutureEntry : client.describeTopics(topicNames).values().entrySet()) {
            topicDescription = kafkaFutureEntry.getValue().get();
            System.out.printf("topic name : %s\n", topicDescription.name());
            retrieveTopic(topicDescription);
        }
    }

    /**
     *
     * show specific topic's information (name, partitons, replications, partition leader, current ISR(in-sync replicas) by name
     *
     * @param topic
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="show specific topic's information", key={"show-topic", "st"})
    public void showTopicInfoByName(String topic) throws ExecutionException, InterruptedException {
        TopicDescription topicDescription = null;
        for (Map.Entry<String, TopicDescription> topicDescEntry : client.describeTopics(Arrays.asList(topic)).all().get().entrySet()) {
            topicDescription = topicDescEntry.getValue();
            System.out.printf("topic name : %s\n", topicDescription.name());
            retrieveTopic(topicDescription);
        }
    }

    /**
     *
     * create topic
     *
     * @param topic topic will be created with this name
     * @param partitions topic's message splits into number of partitons
     * @param replicationFactor each partition duplicated with number of replication factor
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchFieldException
     */
    @ShellMethod(value="create topic by name", key={"create-topic", "ct"})
    public Void createTopic(String topic,
                            @ShellOption(defaultValue = "-1", help = "if not set, use broker default") String partitions,
                            @ShellOption(defaultValue = "-1", help = "if not set, use broker default", value = "replication-factor") String replicationFactor)
                                            throws ExecutionException, InterruptedException, NoSuchFieldException {
        String firstNodeId = String.valueOf(showAllNodesList().stream().collect(Collectors.toList()).get(0).id());
        int partitionNum;
        short replicationNum;
        if("-1".equals(partitions)){
            partitionNum = Integer.parseInt(getBrokerMetricByMetricName(firstNodeId, "num.partitions"));
        } else {
            partitionNum = Integer.parseInt(partitions);
        }
        if("-1".equals(replicationFactor)){
            replicationNum = (short)Integer.parseInt(getBrokerMetricByMetricName(firstNodeId, "default.replication.factor"));
        } else {
            replicationNum = (short)Integer.parseInt(replicationFactor);
        }
        System.out.printf("trying to create topic %s with partition %d replication %d\n", topic, partitionNum, replicationNum);
        CreateTopicsResult result = client.createTopics(Arrays.asList(new NewTopic(topic, partitionNum, replicationNum)));
        return result.all().get();
    }

    /**
     *
     * delete topic by name
     *
     * @param topic
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="delete topic by name", key={"delete-topic", "dt"})
    public Void deleteTopic(String topic) throws ExecutionException, InterruptedException {
        System.out.printf("trying to delete topic %s\n", topic);
        return client.deleteTopics(Arrays.asList(topic)).all().get();
    }

    /**
     *
     * show consumer group's list in this cluster
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="show consumer group list", key={"show-consumer-group-list", "scgl"})
    public void showconsumergroupList() throws ExecutionException, InterruptedException {
        Collection<ConsumerGroupListing> consumerGroupListings = client.listConsumerGroups().all().get();
        System.out.printf("Cluster has %d consumer groups\n", consumerGroupListings.size());
        consumerGroupListings.forEach(value -> {
                System.out.printf("group id : %s / simple group : %s\n",value.groupId(), value.isSimpleConsumerGroup());
        });
    }

    /**
     *
     * @param groupId consumer group's id. you can see at {@link AdminUtils#showconsumergroupList}
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ShellMethod(value="show specific consumer group's detail information", key={"show-consumer-group-info", "scgi"})
    public void showConsumerGroupInfo(String groupId) throws ExecutionException, InterruptedException {
        Map<String, ConsumerGroupDescription> groupDescMap = client.describeConsumerGroups(Arrays.asList(groupId)).all().get();
        for (Map.Entry<String, ConsumerGroupDescription> consumerGroupEntry : groupDescMap.entrySet()) {
            ConsumerGroupDescription groupDesc = consumerGroupEntry.getValue();
            String groupIdValue = groupDesc.groupId();
            int coordinatorId = groupDesc.coordinator().id();
            String partitionAssignor = groupDesc.partitionAssignor();
            String groupStateName = groupDesc.state().name();
            System.out.println("- Group Info");
            System.out.printf("\tgroupId(%s)\tstate(%s)\tcoordinatorId(%d)\tpartitionAssignor(%s)\n", groupIdValue, groupStateName, coordinatorId, partitionAssignor);
            System.out.println("- Member info");
            groupDesc.members().forEach(member -> {
                String memberHost = member.host();
                String clientId = member.clientId();
                String consumerId = member.consumerId();
                System.out.printf("\tmember-host(%s)\tclientId(%s)\tconsumerId(%s)\n",memberHost,clientId,consumerId);
                MemberAssignment assignment = member.assignment();
                System.out.println("- Topic-Partition information");
                for (TopicPartition topicPartition : assignment.topicPartitions()) {
                    String topic = topicPartition.topic();
                    int partition = topicPartition.partition();
                    System.out.printf("\t%s(%d)", topic, partition);
                }
                System.out.println();
            });


        }
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

    public AdminClient getClient() {
        return client;
    }

    public void setClient(AdminClient client) {
        this.client = client;
    }

    @ShellMethodAvailability
    public Availability isAvailable(){
        return connectionStatus.get(ADMIN_MODE) ? Availability.available() : Availability.unavailable("not connected, use 'connect'");
    }
}
