package com.study.dataStreamApi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-13 11:29
 * @action 用于测试的source,从集合获取数据
 */
public class Demo2_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //基于一个集合Collection获取一个Source
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
//        DataStreamSource<Integer> streamSource = env.fromCollection(list);
        //基于元素获取Source
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        streamSource.print();
        env.execute();
    }
}
