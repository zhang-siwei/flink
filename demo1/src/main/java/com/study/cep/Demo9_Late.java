package com.study.cep;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-20 19:30
 * @action  sideOutputLateData(OutputTag<T> lateDataOutputTag)  将迟到数据写入测流
 */
public class Demo9_Late {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "s1".equals(waterSensor.getId());
                    }
                });
        OutputTag<WaterSensor> outputTag = new OutputTag<>("late", TypeInformation.of(WaterSensor.class));
        /*
                只有  数据的eventtime  <= 水印，数据才会被处理。
                水印推进后，迟到对数据，不会被处理的！
         */
        PatternStream<WaterSensor> result = CEP.pattern(ds, pattern)
                //把迟到的数据写入测流
                .sideOutputLateData(outputTag);
        SingleOutputStreamOperator<String> select = result.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        });
        //主流
        select.print().setParallelism(1);
        //测流
        select.getSideOutput(outputTag).printToErr("迟到");
        env.execute();
    }
}
