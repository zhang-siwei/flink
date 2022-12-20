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
 * @time 2022-12-20 18:55
 * @action   times, consecutive , allowCombinations
 *   times() 默认情况,两次之间使用 followedBy进行连接
 *  .consecutive()  要求两个条件之间必须紧挨着,类似next()
 *  .allowCombinations() 类似 followedByAny的效果
 */
public class Demo5_TimesSequence {
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
                //默认情况,两次之间使用 followedBy进行连接
                .times(2)
                //要求两个条件之间必须紧挨着,类似next()
                //.consecutive()
                //类似 followedByAny的效果
                //.allowCombinations()
                ;
        //和上述对 times(2)是等价对
        /*Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                })
                .followedBy("s1_2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                });*/

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
