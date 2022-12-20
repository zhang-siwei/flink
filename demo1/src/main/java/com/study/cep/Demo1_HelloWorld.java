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
 * @time 2022-12-20 16:31
 * @action   cep
 * ①有流，必须有水印。会对数据进行排序。
 * 不支持处理时间。
 * ②制定一个规则Pattern
 * ③将规则作用到流上，普通流变为 规则流
 * ④从规则流中查询出，人家已经给你匹配好的数据
 */
public class Demo1_HelloWorld {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        //①有流,必须有水印
        SingleOutputStreamOperator<WaterSensor> ds = env.readTextFile("data/cep.txt")
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);
        /*
                ②制定一个规则Pattern
                规则的格式:
                        Pattern 以begin开头
                        Pattern1  [连续的规则]  Pattern2   [连续的规则]  Pattern3
         */
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                });
        //③将规则作用到流上，普通流变为 规则流
        PatternStream<WaterSensor> result = CEP.pattern(ds, pattern);
        //④从规则流中查询出，人家已经给你匹配好的数据
        result.select(new PatternSelectFunction<WaterSensor, String>() {
                    /*
                           事件驱动，来一条数据，就匹配一次，如果匹配成功，select就执行一次
                         Map<String, List<WaterSensor>> map
                           key:  规则名
                           value:   匹配到所有对数据
                    */
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        return map.toString();
                    }
                })
                .print();
        env.execute();
    }
}
