package com.study.dataStreamApi.watermark;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-16 19:01
 * @action
 * 水印的产生：
 *         从源头产生，可以在中间位置产生。
 *         从源头产生:
 *         public <OUT> DataStreamSource<OUT> fromSource(
 *        Source<OUT, ?, ?> source,
 *        WatermarkStrategy<OUT> timestampsAndWatermarks,
 *        String sourceName)
 * 水印策略:
 *     WatermarkStrategy<T> forMonotonousTimestamps() : 用于产生连续(不容忍乱序)的水印.
 *                     默认选 事件时间属性 - 1 作为水印
 *     WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness)： 用于提供一个乱序容忍度，产生水印。
 *                 水印基于数据的时间，会推迟 maxOutOfOrderness
 * -----------------------
 *     水印是水印，窗口是窗口。没有直接关系！
 *         水印负责更新算子的时钟，算子的时钟到点，触发基于EventTime窗口的运算。
 *         数据落入哪个窗口取决于 数据的 eventtime(时间属性)。
 */
public class Demo1_Watermark {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3333);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

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
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println(context.window());
                        List<WaterSensor> list = MyUtil.parseIterable(elements);
                        out.collect(list.toString());
                    }
                })
                .print();
        env.execute();
    }
}