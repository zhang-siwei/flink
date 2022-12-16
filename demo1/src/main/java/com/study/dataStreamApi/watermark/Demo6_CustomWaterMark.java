package com.study.dataStreamApi.watermark;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-16 23:37
 * @action 自定义水印生成策略
 */
public class Demo6_CustomWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(1);
        //自定义策略
        //水印具体怎么生成
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                        return new MyWMGeneretor();
                    }
                })
                .withTimestampAssigner((w, l) -> w.getTs());
        env.socketTextStream("localhost", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        System.out.println("数据达到时,算子的水印时钟:" + timerService.currentWatermark());
                        System.out.println("数据达到时,算子的物理时钟:" + timerService.currentProcessingTime());
                        collector.collect(waterSensor);
                    }
                })
                .print();
        env.execute();
    }

    //自动延迟1000ms
    public static class MyWMGeneretor implements WatermarkGenerator<WaterSensor> {
        //用来保存当前subtask收到的最大的eventtime
        private long maxTimestamp = Long.MIN_VALUE;

        //间歇性生成
        @Override
        public void onEvent(WaterSensor waterSensor, long eventtime, WatermarkOutput watermarkOutput) {
            maxTimestamp = Math.max(maxTimestamp, eventtime);
            Watermark watermark = new Watermark(maxTimestamp);
            watermarkOutput.emitWatermark(watermark);
            System.out.println("向下游发送:" + watermark.getTimestamp());
        }

        //周期性生成
        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
//            Watermark watermark = new Watermark(maxTimestamp);
//            watermarkOutput.emitWatermark(watermark);
//            System.out.println("向下游发送:" + watermark.getTimestamp());
        }
    }
}
