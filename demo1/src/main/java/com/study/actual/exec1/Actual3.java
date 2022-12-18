package com.study.actual.exec1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.study.function.MySourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-15 15:45
 * @action  APP各渠道市场推广统计
 */
public class Actual3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new MySourceFunction());
        source.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        return new Tuple2<>(jsonObject.getString("channels") + "_" + jsonObject.getString("behavior"), 1);
                    }
                }).keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
