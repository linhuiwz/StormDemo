package com.lh.storm2;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * TopicMsgBolt类是从storm.kafka.KafkaSpout接收数据的Bolt，对接收到的数据进行处理，
 * 然后向后传输给storm.kafka.bolt.KafkaBolt
 */
@Slf4j
public class TopicMsgBolt extends BaseBasicBolt {
    public void execute(Tuple input, BasicOutputCollector collector) {

        String word = (String) input.getValue(0);
        String out = "Message got is '" + word + "'!";
        log.warn("TopicMsgBolt out={}", out);
        collector.emit(new Values(out));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}
