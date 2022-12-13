package com.study.dataStreamApi.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:42
 * @action 算子 union 练习
 * 并集。
*  *          同流合污。
*  *              流中元素的类型相同，才能将多个流合并为一个流。
 */
public class Demo5_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> streamSource1 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> streamSource2 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> streamSource3 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> streamSource4 = env.fromElements("a","b");
        streamSource1.union(streamSource2).print();
        //可以合并多个
        //streamSource1.union(streamSource2,streamSource3).print();
        //1 和 4 不能合并,类型不同
        //streamSource1.union(streamSource4).print();
        env.execute();
    }
}
