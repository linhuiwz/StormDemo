package com.lh.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * 订阅 split sentence bolt的输出流，实现单词计数，并发送当前计数给下一个bolt
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;

    private HashMap<String, Long> counts = null;

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        counts = new HashMap<String, Long>();

    }

    public void execute(Tuple input) {

        String word = input.getStringByField("word");
        Long count = counts.get(word);
        if (count == null) {
            counts.put(word, 1L);
        } else {
            count++;
            counts.put(word, count);
        }
        this.collector.emit(new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
