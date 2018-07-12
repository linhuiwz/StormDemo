package com.lh.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

public class KafkaStormSample {

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        String zkConnString = "localhost:2181";
        String topic = "my-first-topic";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.ignoreZkOffsets = true;
        /**
         * SchemeAsMultiScheme是一个接口，用于指示如何将从Kafka中消耗的ByteBuffer转换为风暴元组。
         * 它源自MultiScheme并接受Scheme类的实现。 有很多Scheme类的实现，一个这样的实现是StringScheme，
         * 它将字节解析为一个简单的字符串。 它还控制输出字段的命名。 签名定义如下。
         */
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("word-spitter", new SplitSentenceBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new WordCountBolt()).shuffleGrouping("word-spitter");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStormSample", config, builder.createTopology());

        Thread.sleep(100000);

        cluster.shutdown();
    }
}
