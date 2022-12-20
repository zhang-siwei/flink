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
 * @time 2022-12-20 19:05
 * @action  greedy()
 *  贪婪模式
 * 单个模式加不加greedy无所谓,贪婪模式只是用于模式序列
 * 尽量使用最多次匹配(去除被包含在其他结果中的结果)
 */
public class Demo6_Greedy {
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
                .timesOrMore(2)
                // 贪婪模式
                //单个模式加不加greedy无所谓,贪婪模式只是用于模式序列
                //尽量使用最多次匹配(去除被包含在其他结果中的结果)
                .greedy()
                .followedBy("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s2".equals(waterSensor.getId());
                    }
                })
                ;
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
