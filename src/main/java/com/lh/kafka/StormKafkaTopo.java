package com.lh.kafka;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;

public class StormKafkaTopo {
    public static void main(String[] args) {
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");

        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "topic1", "/kafka", "kafkaspout");

        Config conf = new Config();
        Map<String, String> map = new HashMap<String, String>();

        map.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "topic2");

        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", (IBasicBolt) new SenqueceBolt()).shuffleGrouping("spout");
        builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");

        if (args != null && args.length > 0) {
            //提交到集群运行
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            //本地模式运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Topotest1121", conf, builder.createTopology());
            Utils.sleep(1000000);
            cluster.killTopology("Topotest1121");
            cluster.shutdown();
        }


    }
}
