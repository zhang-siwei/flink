package com.study.dataStreamApi.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:42
 * @action  算子 map 练习
 */
public class Demo1_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        streamSource.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer integer) throws Exception {
                return "num: " + integer;
            }
        }).print();
        env.execute();
    }
}
