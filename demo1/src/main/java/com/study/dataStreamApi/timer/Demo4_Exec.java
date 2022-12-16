package com.study.dataStreamApi.timer;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-17 0:30
 * @action 定时器练习
 * 思路： 使用基于事件时间的定时器解决。
 * *          第一个传感器到达时，注册一个5s后触发的定时器。
 * *              定时器触发之前，到达的传感器都和前一个水位进行对比，
 * *                  如果超过，无操作
 * *                  如果下降，就删除之前定的定时器。
 * *
 * *              不管定时器被删除，还是被触发。一切归零，下一个传感器到达后，重新制定定时器。
 * *
 * *  --------------------------
 * *      缓存的数据:
 * *             保存之前注册的定时器的触发时间
 * *             保存上一个传感器的水位
 * *             保存一个标记(声明当前传感器是不是第一个)
 * *
 * *
 * *    -----------
 * *      先用一种key
 */
public class Demo4_Exec {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        env.socketTextStream("localhost", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("连续5s上升预警..........");
                        isFirst=true;
                    }

                    //保存之前注册的定时器的触发时间
                    long time = 0L;
                    //保存上一个传感器的水位
                    int lastvc = 0;
                    //保存一个标记(声明当前传感器是不是第一个)
                    boolean isFirst = true;

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        if (isFirst) {
                            //第一个,注册定时器
                            time = context.timestamp() + 5000;
                            timerService.registerEventTimeTimer(time);
                            System.out.println("注册了定时器:" + time);
                            isFirst = false;
                        } else {
                            if (waterSensor.getVc() < lastvc) {
                                timerService.deleteEventTimeTimer(time);
                                System.out.println("删除了定时器:" + time);
                                //重置归0
                                isFirst = true;
                            }
                        }
                        lastvc = waterSensor.getVc();
                    }
                })
                .print();
        env.execute();
    }
}
