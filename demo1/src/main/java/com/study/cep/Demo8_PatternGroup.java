package com.study.cep;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-20 19:18
 * @action  GroupPattern 模式组
 */
public class Demo8_PatternGroup {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        SingleOutputStreamOperator<WaterSensor> ds = env.readTextFile("data/cep.txt")
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);

    /*
                s1s2s1s2

                [s1s2] 重复2次

                S1S2S1S2S1S2S1S2...

                [s1s2] 重复4次

                把多个模式组合在一起，形成一个模式组。
                以模式组为单位，让它重复N次
         */
        //构造基本模式
        Pattern<WaterSensor, WaterSensor> pattern1 = Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                })
                .next("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s2".equals(waterSensor.getId());
                    }
                });
        //封装为模式组
        GroupPattern<WaterSensor, WaterSensor> groupPattern = Pattern.begin(pattern1);
        //以组为单位重复N次
       /*输出的结果以模式排序
       {s1=[WaterSensor(id=s1, ts=1004, vc=30), WaterSensor(id=s1, ts=6666, vc=10)],
        s2=[WaterSensor(id=s2, ts=4000, vc=10), WaterSensor(id=s2, ts=7777, vc=10)]}*/
        Pattern<WaterSensor, WaterSensor> pattern = groupPattern.times(2);

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
