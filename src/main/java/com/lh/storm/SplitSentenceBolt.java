package com.lh.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 订阅sentence spout发射的tuple流，实现分割单词
 */
public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.collector = collector;

    }

    public void execute(Tuple input) {
        String sentence = input.getStringByField("sentence");
        //String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for (String word : words) {
            this.collector.emit(new Values(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
