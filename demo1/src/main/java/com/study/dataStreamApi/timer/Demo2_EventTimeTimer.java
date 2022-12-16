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
 * @time 2022-12-17 0:09
 * @action  基于水印的定时器
 * 如果制定的是过去时间的定时器，那么只要当前的水印推进，也会立刻触发！
 */
public class Demo2_EventTimeTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());
        env.socketTextStream("localhost", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("预警..........");
                    }

                    long time = 0L; //计时

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        if (waterSensor.getVc() > 30) {
                            time = context.timestamp() + 5000;
                            timerService.registerEventTimeTimer(time);
                            System.out.println("注册了定时器:" + time);
                        } else if (waterSensor.getVc() <= 10) {
                            timerService.deleteEventTimeTimer(time);
                            System.out.println("删除了定时器:" + time);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
