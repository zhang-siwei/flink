package com.study.dataStreamApi.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:42
 * @action 算子 FlatMap 练习
 */
public class Demo4_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        streamSource.flatMap(new FlatMapFunction<Integer, String>() {
            @Override
            public void flatMap(Integer integer, Collector<String> collector) throws Exception {
                if (integer %2==0) {
                    collector.collect("偶数:"+integer);
                }
            }
        }).print();
        env.execute();
    }
}
