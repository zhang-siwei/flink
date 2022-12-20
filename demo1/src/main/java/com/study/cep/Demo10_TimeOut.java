package com.study.cep;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-20 19:47
 * @action
 */
public class Demo10_TimeOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        SingleOutputStreamOperator<WaterSensor> ds = env.readTextFile("data/ceptimeout.txt")
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                })
                //下条数据必须在2s内到达
                .within(Time.seconds(2))
                .next("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s2".equals(waterSensor.getId());
                    }
                });

        OutputTag<String> outputTag = new OutputTag<>("timeout", TypeInformation.of(String.class));
        PatternStream<WaterSensor> result = CEP.pattern(ds, pattern);
        SingleOutputStreamOperator<String> select = result.select(
                outputTag,
                new PatternTimeoutFunction<WaterSensor, String>() {
                    //Map<String, List<WaterSensor>> map: 匹配的是超时的数据
                    //超时数据: 第一条数据
                    //超时数据自动写入测流,只写入超时的数据(第一条数据)
                    @Override
                    public String timeout(Map<String, List<WaterSensor>> map, long l) throws Exception {
                        return map.toString();
                    }
                },
                new PatternSelectFunction<WaterSensor, String>() {
                    //匹配的是未超时的数据
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        return map.toString();
                    }
                });
        //主流
        select.print();
        //测流
        select.getSideOutput(outputTag).printToErr("超时");
        env.execute();
    }
}
