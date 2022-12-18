package com.study.actual.exec1;

import com.study.actual.function.UserBehaviorMapFun;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * @author zhang.siwei
 * @time 2022-12-13 21:00
 * @action  计算独立 uv
 */
public class Actual2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("data/UserBehavior.csv")
                .map(new UserBehaviorMapFun())
                .filter(u -> "pv".equals(u.getBehavior()))
                .map(u ->u.getUserId())
                .process(new ProcessFunction<Long, Integer>() {
                    Set<Long> set = new HashSet<>();
                    @Override
                    public void processElement(Long value, ProcessFunction<Long, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                        if (set.add(value)) out.collect(set.size());
                    }
                }).setParallelism(1)
                .print();
        env.execute();
    }
}
