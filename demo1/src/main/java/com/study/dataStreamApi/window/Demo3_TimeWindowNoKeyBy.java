package com.study.dataStreamApi.window;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-15 19:30
 * @action 基于时间的
 * 按照性质：基于时间
 * api: Window
 * size： 由时间范围决定 [start,end)
 * slide： 到点(end)就算。
 * flink怎么知道时间是多少?
 * processingTime(演示):  处理时间。参考计算机的物理时钟！
 * eventTime:   从数据中提取属性作为时间。
 * 按照计算方式： 不keyBy
 * xxxAll
 * 按照功能:
 * 滑动|滚动|会话
 */
public class Demo3_TimeWindowNoKeyBy {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                //基于处理时间的 滚动窗口。 从1970-1-1 0:0:0 每间隔10s就是一个窗口
                //.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //基于处理时间的 滑动窗口
                //.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                //会话窗口  下一条数据和上一条数据的时间间隔在5s内，窗口永远不关闭，无法计算
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                        //获取当前时间窗口
                        TimeWindow window = context.window();

                        MyUtil.printTimeWindow(window);

                        out.collect(MyUtil.parseIterable(elements).toString());
                    }
                })
                .print();
    }
}
