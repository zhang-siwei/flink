package com.study.cep;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.*;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.functions.adaptors.PatternFlatSelectAdapter;
import org.apache.flink.cep.functions.adaptors.PatternTimeoutFlatSelectAdapter;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-20 20:04
 * @action  超时处理,自定义超时处理
 */
public class Demo11_TimeOutProcess {
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
        SingleOutputStreamOperator<String> select = result.process(new MyProcess());
        //主流
        select.print();
        //测流
        select.getSideOutput(outputTag).printToErr("超时");
        env.execute();
    }

    public static class MyProcess extends PatternProcessFunction<WaterSensor,String> implements TimedOutPartialMatchHandler<WaterSensor>{
        //处理未超时匹配到的数据
        @Override
        public void processMatch(Map<String, List<WaterSensor>> map, Context context, Collector<String> collector) throws Exception {
            collector.collect(map.toString());
        }
        //处理超时的数据
        @Override
        public void processTimedOutMatch(Map<String, List<WaterSensor>> map, Context context) throws Exception {
            context.output(new OutputTag<>("timeout", TypeInformation.of(String.class)),map.toString());
        }
    }
}
