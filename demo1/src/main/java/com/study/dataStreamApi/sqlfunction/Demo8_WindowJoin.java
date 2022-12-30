package com.study.dataStreamApi.sqlfunction;


import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by Smexy on 2022/12/23
 *
 */
public class Demo8_WindowJoin
{
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
            .withTimestampAssigner( (e, r) -> e.getTs());


        env.setParallelism(1);

        //准备两个流
        SingleOutputStreamOperator<WaterSensor> ds1 = env
            .socketTextStream("hadoop102", 8888)
            .map(new WaterSensorMapFunction())
            .assignTimestampsAndWatermarks(watermarkStrategy);

        SingleOutputStreamOperator<WaterSensor> ds2 = env
            .socketTextStream("hadoop102", 8889)
            .map(new WaterSensorMapFunction())
            .assignTimestampsAndWatermarks(watermarkStrategy);

        /*
                join

                select
                    a.xxx
                from
                a join b on a.id = b.id
         */
        ds1.join(ds2)
           //左边的流取什么字段作为 on的key
           .where(WaterSensor::getId)
           .equalTo(WaterSensor::getId)
           .window(TumblingEventTimeWindows.of(Time.seconds(5)))
           .apply(new JoinFunction<WaterSensor, WaterSensor, String>()
           {
               @Override
               public String join(WaterSensor first, WaterSensor second) throws Exception {
                   return first + "===" + second;
               }
           })
           .print();

        env.execute();

    }
}
