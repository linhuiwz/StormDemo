package com.lh.storm2;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * 订阅kafka数据的处理参数
 */
@Slf4j
public class MessageScheme implements Scheme {
    public List<Object> deserialize(ByteBuffer ser) {
        try {
            String msg = new String(ser.array(), "UTF-8");
            log.warn("MessageScheme get one message is {}", msg);
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    public Fields getOutputFields() {
        /**
         * 需要与接收数据的Bolt中统一（在这个例子中可以不统一，因为后面直接取第0条数据，
         * 但是在wordCount的那个例子中就需要统一了
         */
        return new Fields("msg");
    }
}
