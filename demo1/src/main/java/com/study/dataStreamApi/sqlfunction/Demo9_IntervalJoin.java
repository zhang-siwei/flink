package com.study.dataStreamApi.sqlfunction;


import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by Smexy on 2022/12/23
 */
public class Demo9_IntervalJoin
{
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
            .withTimestampAssigner( (e, r) -> e.getTs());


        env.setParallelism(1);

        //准备两个流  必须是keyby
        KeyedStream<WaterSensor, String> ds1 = env
            .socketTextStream("hadoop102", 8888)
            .map(new WaterSensorMapFunction())
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .keyBy(WaterSensor::getId);

        KeyedStream<WaterSensor, String> ds2 = env
            .socketTextStream("hadoop102", 8889)
            .map(new WaterSensorMapFunction())
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .keyBy(WaterSensor::getId);


        //进行interval join
        ds1.intervalJoin(ds2)
           //用了就会报错，不支持ProcessingTime
           //Time-bounded stream joins are only supported in event time
           //.inProcessingTime()
            //当前ds1中的每一条数据和 ds2哪个范围的数据进行关联
           .between(Time.seconds(-5),Time.seconds(5))
           //如果一条数据刚好卡在 边界上，默认是 包含。可以设置不包含
           /* .lowerBoundExclusive()
            .upperBoundExclusive()*/
           // 左右两侧到达的数据的eventtime，如果 <  process的水印，算是迟到的数据，无法计算！
           .process(new ProcessJoinFunction<WaterSensor, WaterSensor, String>()
           {
               @Override
               public void processElement(WaterSensor left, WaterSensor right, Context ctx, Collector<String> out) throws Exception {
                   out.collect( left + "===" + right);
               }
           })
           .print();

        env.execute();

    }
}
