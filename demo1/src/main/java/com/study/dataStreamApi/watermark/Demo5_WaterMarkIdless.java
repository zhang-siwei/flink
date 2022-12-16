package com.study.dataStreamApi.watermark;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.security.spec.EncodedKeySpec;
import java.time.Duration;

/**
 * @author zhang.siwei
 * @time 2022-12-16 23:22
 * @action  水印传递间隔
 * 如果下游从上有的多个并行度获取数据，假设上游的某个subTask迟迟没有数据产生，导致watermark无法更新和推进，
 * 设置一个时间间隔，超过这个时间间隔，上游的某个subTask依旧没有数据产生，就退出水印的传递。
 */
public class Demo5_WaterMarkIdless {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(2);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs())
                //上游的某个subTask 10秒内没有新的数据产生，退出水印的传递。赋闲。直到有新的数据产生，还可以继续传递水印
                .withIdleness(Duration.ofSeconds(10));
        env.socketTextStream("localhost", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        System.out.println("数据达到时,算子的水印时钟:"+timerService.currentWatermark());
                        collector.collect(waterSensor);
                    }
                })
                .print();
        env.execute();
    }
}
