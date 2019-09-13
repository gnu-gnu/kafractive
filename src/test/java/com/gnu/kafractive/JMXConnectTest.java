package com.gnu.kafractive;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JMXConnectTest {
    static class ClientListener implements NotificationListener {

        public void handleNotification(Notification notification,
                                       Object handback) {
            System.out.println("\nReceived notification:");
            System.out.println("\tClassName: " + notification.getClass().getName());
            System.out.println("\tSource: " + notification.getSource());
            System.out.println("\tType: " + notification.getType());
            System.out.println("\tMessage: " + notification.getMessage());
            if (notification instanceof AttributeChangeNotification) {
                AttributeChangeNotification acn =
                        (AttributeChangeNotification) notification;
                System.out.println("\tAttributeName: " + acn.getAttributeName());
                System.out.println("\tAttributeType: " + acn.getAttributeType());
                System.out.println("\tNewValue: " + acn.getNewValue());
                System.out.println("\tOldValue: " + acn.getOldValue());
            }
        }
    }

    private static class KafkaMbean implements DynamicMBean {
        private final ObjectName objectName;
        private final Map<String, KafkaMetric> metrics = new HashMap();

        public KafkaMbean(String mbeanName) throws MalformedObjectNameException {
            this.objectName = new ObjectName(mbeanName);
        }

        public ObjectName name() {
            return this.objectName;
        }

        public void setAttribute(String name, KafkaMetric metric) {
            this.metrics.put(name, metric);
        }

        public Object getAttribute(String name) throws AttributeNotFoundException, MBeanException, ReflectionException {
            if (this.metrics.containsKey(name)) {
                return ((KafkaMetric)this.metrics.get(name)).metricValue();
            } else {
                throw new AttributeNotFoundException("Could not find attribute " + name);
            }
        }

        public AttributeList getAttributes(String[] names) {
            AttributeList list = new AttributeList();
            String[] var3 = names;
            int var4 = names.length;

            for(int var5 = 0; var5 < var4; ++var5) {
                String name = var3[var5];

                try {
                    list.add(new Attribute(name, this.getAttribute(name)));
                } catch (Exception var8) {
                    System.out.printf("Error getting JMX attribute '%s'", name);
                }
            }

            return list;
        }

        public KafkaMetric removeAttribute(String name) {
            return (KafkaMetric)this.metrics.remove(name);
        }

        public MBeanInfo getMBeanInfo() {
            MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[this.metrics.size()];
            int i = 0;

            for(Iterator var3 = this.metrics.entrySet().iterator(); var3.hasNext(); ++i) {
                Map.Entry<String, KafkaMetric> entry = (Map.Entry)var3.next();
                String attribute = (String)entry.getKey();
                KafkaMetric metric = (KafkaMetric)entry.getValue();
                attrs[i] = new MBeanAttributeInfo(attribute, Double.TYPE.getName(), metric.metricName().description(), true, false, false);
            }

            return new MBeanInfo(this.getClass().getName(), "", attrs, (MBeanConstructorInfo[])null, (MBeanOperationInfo[])null, (MBeanNotificationInfo[])null);
        }

        public Object invoke(String name, Object[] params, String[] sig) throws MBeanException, ReflectionException {
            throw new UnsupportedOperationException("Set not allowed.");
        }

        public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
            throw new UnsupportedOperationException("Set not allowed.");
        }

        public AttributeList setAttributes(AttributeList list) {
            throw new UnsupportedOperationException("Set not allowed.");
        }
    }

    private interface TestBean {
        public int Value();
    }

    public static void main(String[] args) throws IOException, MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, IntrospectionException {
        ClientListener listener = new ClientListener();

        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://192.168.0.201:19092/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(url);
        MBeanServerConnection mBeanServerConnection = connector.getMBeanServerConnection();
        // Get All names
        /*GetAllNames(mBeanServerConnection);
        queryMbean(mBeanServerConnection);
        mBeanAttr(mBeanServerConnection, name);*/
        String activeControllerCount = "kafka.controller:type=KafkaController,name=ActiveControllerCount";
        String underReplicatedPartitions = "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions";
        String OfflineReplicaCount = "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions";
        System.out.println(getJMXObject(mBeanServerConnection, activeControllerCount));
        System.out.println(getJMXObject(mBeanServerConnection, underReplicatedPartitions));
        System.out.println(getJMXObject(mBeanServerConnection, OfflineReplicaCount));


        connector.close();


    }

    private static Object getJMXObject(MBeanServerConnection mBeanServerConnection, String name) throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException, IOException, MalformedObjectNameException {
        return mBeanServerConnection.getAttribute(new ObjectName(name), "Value");
    }

    private static void mBeanAttr(MBeanServerConnection mBeanServerConnection, String name) throws InstanceNotFoundException, IntrospectionException, ReflectionException, IOException, MalformedObjectNameException {
        MBeanInfo mBeanInfo = mBeanServerConnection.getMBeanInfo(ObjectName.getInstance(name));
        for (MBeanAttributeInfo attribute : mBeanInfo.getAttributes()) {
            System.out.println(attribute);
        }
    }

    private static void queryMbean(MBeanServerConnection mBeanServerConnection) throws IOException {
        for (ObjectInstance queryMBean : mBeanServerConnection.queryMBeans(null, null)) {
            System.out.println(queryMBean);
        }
    }

    private static void GetAllNames(MBeanServerConnection mBeanServerConnection) throws IOException {
        for (ObjectName queryName : mBeanServerConnection.queryNames(null, null)) {
            System.out.println(queryName.getCanonicalName());
        }
    }
}
