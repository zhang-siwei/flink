package com.study.cep;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-20 18:32
 * @action 模式序列。
 * 多个模式。
 * 指定多个模式，如何进行串联(连接)
 *
 *
 * CEP 对原理基于 NFA 设计
 */
public class Demo4_PatternSequence {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        SingleOutputStreamOperator<WaterSensor> ds = env.readTextFile("data/cep.txt")
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                })
                // s2  条件的名字
                //条件s1匹配的数据 接下里紧挨着的数据必须 匹配条件s2     输出两个模式的数据   严格连续
                .next("s2")
                //条件s1匹配的数据 接下里紧挨着的数据必须 不匹配条件s2     输出第一个模式的数据   严格不连续
                //.notNext("s2")
                //条件s1匹配的数据 后续数据中有一条匹配条件s2      输出两个模式的数据(第二个模式的数据以匹配到的第一条为准) 松散连续
                //.followedBy("s2")
                //条件s1匹配的数据 后续数据中没有一条匹配条件s2    输出第一个模式的数据和后续模式的数据 松散不连续
                //报错:NotFollowedBy is not supported as a last part of a Pattern!（不能作为条件的最后一部分，可以作为中间部分）,流的数据是源源不断的来，无法保证后续的数据是否一定不是s2
                //.notFollowedBy("s2")   //后面需要有结束,如后面加上条件3
                //条件s1匹配的数据 和后续任何一条匹配条件s2的数据 配对     输出两个模式的数据
                //.followedByAny("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s2".equals(waterSensor.getId());
                    }
                })
                /*.followedBy("s3")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s3".equals(waterSensor.getId());
                    }
                })*/;

        PatternStream<WaterSensor> result = CEP.pattern(ds, pattern);
        result.select(new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        return map.toString();
                    }
                })
                .print();
        env.execute();
    }
}
