package com.lh.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * BaseRichSpout是ISpout接口和IComponent接口的简单实现，接口对用不到的方法提供了默认的实现
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private int index = 0;

    private String[] sentences = {
            "my name is soul",
            "im a boy",
            "i have a dog",
            "my dog has fleas",
            "my girl friend is beautiful"
    };

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {

        this.collector = collector;

    }

    public void nextTuple() {

        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }

        Utils.sleep(1);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        //告诉组件发出数据流包含sentence字段
        declarer.declare(new Fields("sentence"));

    }
}
