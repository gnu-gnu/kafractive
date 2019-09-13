# Kafractive

kafractive is Kafka admin CLI tool based on top of Spring shell
 
currently supports AdminClient, Consumer and showing Consumer-metrics during consume jobs running
 
Execute example 1 : need to specify bootstrap server in your shell
```
java -jar kafractive-0.1.2.0-SNAPSHOT.jar
```

Execute example 2 : pass server list file argument
```
java -jar kafractive-0.1.2.0-SNAPSHOT.jar server.list
```
```
# example of server.list
192.168.0.201:9092
192.168.0.202:9092
192.168.0.203:9092
``` 

AVAILABLE COMMANDS
```
Admin Connector
        ac, admin-connect: connect Brokers with admin client
        aclose, admin-close: disconnect

Admin Utils
      * create-topic, ct: create topic by name
      * delete-topic, dt: delete topic by name
      * san, show-all-nodes: show all nodes
      * sat, show-all-topics: show all topic's information in cluster
      * sc, show-controller: get controller info
      * scgi, show-consumer-group-info: show specific consumer group's detail information
      * scgl, show-consumer-group-list: show consumer group list
      * show-node-config, snc: show specific node's config
      * show-topic, st: show specific topic's information
      * show-topic-config, stc: show specific topic's config

Built-In Commands
        clear: Clear the shell screen.
        exit, quit: Exit the shell.
        help: Display help about available commands.
        script: Read and execute commands from a file.
        stacktrace: Display the full stacktrace of the last error.

Common Properties
        show-status: show common status

Consumer Running Thread
      * scm, show-consumer-metrics: show subscriptions of running comsumer
      * show-subscription, ss: show subscriptions of running comsumer

Consumer Utils
        cc, consumer-connect: connect between consumer and broker
      * cclose, consumer-close: close consumer

```