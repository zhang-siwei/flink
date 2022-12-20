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
 * @action   cep times
 * 定义的where条件应用两次去匹配
*  .times(2)
*  定义的条件应用2次或三次
*  .times(2,3)
*  定义的条件应用2次或N(N>2)次,危险,第一条数据要一直缓存.不断地和后续的数据进行匹配,一般和终止条件仪一起使用
*  .timesOrMore(2)
*  定义的条件应用1次或N(N>2)次,危险,第一条数据要一直缓存.不断地和后续的数据进行匹配,一般和终止条件仪一起使用
*  .oneOrMore()
*  到...为止,截止数据不匹配
*  .until(IterativeCondition<F>)
 */
public class Demo2_Time {
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
                //定义的where条件应用两次去匹配
                //.times(2)
                //定义的条件应用2次或三次
                //.times(2,3)
                //定义的条件应用2次或N(N>2)次,危险,第一条数据要一直缓存.不断地和后续的数据进行匹配,一般和终止条件仪一起使用
                .timesOrMore(2)
                //定义的条件应用1次或N(N>2)次,危险,第一条数据要一直缓存.不断地和后续的数据进行匹配,一般和终止条件仪一起使用
                //.oneOrMore()
                //到...为止,截止数据不匹配
                .until(new SimpleCondition<WaterSensor>() {
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
