package com.study.dataStreamApi.timer;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @author zhang.siwei
 * @time 2022-12-16 23:58
 * @action  基于物理时钟的定时器
 * 定时器：
 *  *              到点就执行指定的逻辑
 *  *
 *  *       编程： ①定义定时器
 *  *             ②到点自动触发
 *  *                  基于processingTime
 *  *                  基于EventTime
 *  *
 *  *
 *  *     -------------
 *  *      一旦某个传感器的水位超过30，就注册一个10s后报警的定时器。
 *  *          在报警前，一旦水位降到10以下，就取消之前的预警。
 *  *
 *  *
 *  *          UnsupportedOperationException: Deleting timers is only supported on a keyed streams.
 *  *                  删除定时器，必须keyBy
 *  *
 *  *
 *  *                  先使用同一种key！
 */
public class Demo1_ProcessingTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("预警.........");
                    }
                    long time=0L; //计时
                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        if (waterSensor.getVc()>30){
                            time=timerService.currentProcessingTime()+10000;
                            timerService.registerProcessingTimeTimer(time);
                            System.out.println("注册了定时器:"+time);
                        }else if (waterSensor.getVc()<=10){
                            timerService.deleteProcessingTimeTimer(time);
                            System.out.println("删除了定时器:"+time);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
