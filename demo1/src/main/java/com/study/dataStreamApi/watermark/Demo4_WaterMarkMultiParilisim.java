package com.study.dataStreamApi.watermark;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-16 22:59
 * @action 算子的水印时钟分别发送到多个下游,每个下游取收到的最小水印作为自己的时间
 * *  上游SubTask如果需要把数据发送到多个下游的SubTask，那么会对水印进行广播下游会取上游所有水印中的最小的那个作为水印
 * 数据-----------------------------------------------------
 *  s1,400,1
 *  s1,500,2
 *  s1,200,3
 *  s1,600,4
 *  s1,700,5
 *  s1,800,6
 *  s1,900,7
 *结果:---------------------------------------------------------
 * WaterSensor(id=s1, ts=400, vc=1)来了,更新maxTimestamp:400
 * 当前向下游发送水印:399
 * 当前数据达到时,这个算子的水印时钟:-9223372036854775808
 * 2> WaterSensor(id=s1, ts=400, vc=1)
 * WaterSensor(id=s1, ts=500, vc=2)来了,更新maxTimestamp:500
 * 当前向下游发送水印:499
 * 当前数据达到时,这个算子的水印时钟:-9223372036854775808
 * 2> WaterSensor(id=s1, ts=500, vc=2)
 * WaterSensor(id=s1, ts=200, vc=3)来了,更新maxTimestamp:400
 * 当前向下游发送水印:399
 * 当前数据达到时,这个算子的水印时钟:399
 * 2> WaterSensor(id=s1, ts=200, vc=3)
 * WaterSensor(id=s1, ts=600, vc=4)来了,更新maxTimestamp:600
 * 当前向下游发送水印:599
 * 当前数据达到时,这个算子的水印时钟:399
 * 2> WaterSensor(id=s1, ts=600, vc=4)
 * WaterSensor(id=s1, ts=700, vc=5)来了,更新maxTimestamp:700
 * 当前向下游发送水印:699
 * 当前数据达到时,这个算子的水印时钟:399
 * 2> WaterSensor(id=s1, ts=700, vc=5)
 * WaterSensor(id=s1, ts=800, vc=6)来了,更新maxTimestamp:800
 * 当前向下游发送水印:799
 * 当前数据达到时,这个算子的水印时钟:599
 * 2> WaterSensor(id=s1, ts=800, vc=6)
 * WaterSensor(id=s1, ts=900, vc=7)来了,更新maxTimestamp:900
 * 当前向下游发送水印:899
 * 当前数据达到时,这个算子的水印时钟:699
 * 2> WaterSensor(id=s1, ts=900, vc=7)
 * WaterSensor(id=s1, ts=1000, vc=8)来了,更新maxTimestamp:1000
 * 当前向下游发送水印:999
 * 当前数据达到时,这个算子的水印时钟:799
 * 2> WaterSensor(id=s1, ts=1000, vc=8)
 */
public class Demo4_WaterMarkMultiParilisim {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(2);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs();
                    }
                });
        env.socketTextStream("localhost", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        // currentWatermark: 数据达到时，算子的当前时钟
                        System.out.println("当前数据达到时,这个算子的水印时钟:" + timerService.currentWatermark());
                        collector.collect(waterSensor);
                    }
                })
                .print();
        env.execute();
    }
}
