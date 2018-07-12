package com.lh.storm2;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Properties;

public class TopicMsgTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // 配置Zookeeper地址
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        //String topic = "my-first-topic";
        String topic = "logstashtest";
        /**
         * 构造方法第一个参数为上述的storm.kafka.ZkHosts对象，第二个为待订阅的topic名称，
         * 第三个参数zkRoot为写读取topic时的偏移量offset数据的节点（zk node），
         * 第四个参数为该节点上的次级节点名（有个地方说这个是spout的id）。
         */
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, "/" + topic, "topicMsgTopology");
        // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();
        Properties props = new Properties();
        // 配置Kafka broker地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        // serializer.class为消息的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put("kafka.broker.properties", props);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "my-second-topic");
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("msgKafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("msgSentenceBolt", new TopicMsgBolt()).shuffleGrouping("msgKafkaSpout");
        KafkaBolt kafkaBolt = new KafkaBolt<String, Integer>();
        kafkaBolt.withProducerProperties(props);
        builder.setBolt("msgKafkaBolt", kafkaBolt).shuffleGrouping("msgSentenceBolt");
        if (args.length == 0) {
            String topologyName = "kafkaTopicTopology";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Utils.sleep(1000000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } else {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
