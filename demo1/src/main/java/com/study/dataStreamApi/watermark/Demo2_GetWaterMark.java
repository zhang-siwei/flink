package com.study.dataStreamApi.watermark;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-16 19:38
 * @action
 *  *  默认水印时周期性(200ms)产生,当没有数据时，会向流中发送 -Long.max
 */
public class Demo2_GetWaterMark {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3333);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        //设置水印的发送间隔 2000ms
        env.getConfig().setAutoWatermarkInterval(2000);
        //创建水印产生策略
        //从数据中抽取属性作为时间戳(ms)
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs();
                    }
                });
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        //时间服务 1.获取水印 2.注册定时器
                        TimerService timerService = ctx.timerService();

                        //currentWatermark:数据达到时,算子的当前时钟
                        System.out.println("数据达到时,这个算子的水印时钟:"+timerService.currentWatermark());
                        System.out.println("数据达到时,这个算子的物理时钟:"+timerService.currentProcessingTime());
                        out.collect(value);
                    }
                }).startNewChain()
                .print();
        env.execute();
    }
}
