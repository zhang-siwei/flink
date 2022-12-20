package com.study.cep;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-20 21:17
 * @action
 */
public class Demo12_Skip {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        SingleOutputStreamOperator<WaterSensor> ds = env.readTextFile("data/cep.txt")
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);

         /*
             begin(final String name,
                   final AfterMatchSkipStrategy afterMatchSkipStrategy)

                 AfterMatchSkipStrategy: 匹配后的跳过策略，可以设置，将匹配的结果过滤！

                 AfterMatchSkipStrategy.noSkip(): 默认

                 NO_SKIP: 每个成功的匹配都会被输出。
                 SKIP_TO_NEXT: 丢弃以相同事件开始的所有部分匹配。
                 SKIP_PAST_LAST_EVENT: 丢弃起始在这个匹配的开始和结束之间的所有部分匹配
         */
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("s1",AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                })
                .oneOrMore()
                .followedBy("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s2".equals(waterSensor.getId());
                    }
                });
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
