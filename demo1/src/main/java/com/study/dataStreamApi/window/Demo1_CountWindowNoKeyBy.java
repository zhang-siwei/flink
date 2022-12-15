package com.study.dataStreamApi.window;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-15 16:45
 * @action  不带keybuy的基于元素个数的窗口
 * 性质:基于元素个数的窗口
 * countWindow
 * 计算： 不KeyBy
 * xxxAll
 * 功能:  滑动
 * -----------
 * 不KeyBy如何理解？
 * 可以keyBy，但是keyBy无意义！
 * 原因：  xxxAll算子，下游只能有一个process！是否keyBy不影响！
 */
public class Demo1_CountWindowNoKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                //.keyBy(WaterSensor::getId)   不影响,下游并行度为1
                //.countWindowAll(3)  //滚动
                //.countWindowAll(3,2)  //滑动,滑的小 ,重复计算
                .countWindowAll(3,4) //滑动,滑的大,漏算
                /*
                            <IN, OUT, W extends Window>
                                IN: 窗口中的元素类型
                                OUT: 输出类型
                                W ： TimeWindow(有时间范围的窗口):  基于时间窗口。
                                            包含了一组时间属性。
                                            public TimeWindow(long start, long end)

                                     GlobalWindow： 没有时间的窗口，基于个数的窗口

                           The parallelism of non parallel operator must be 1.
                    */
                .process(new ProcessAllWindowFunction<WaterSensor, String, GlobalWindow>() {
                    /*
                            Context context: 应用上下文
                            Iterable<WaterSensor> elements：  把窗口中的元素组织为一个可迭代的集合
                            Collector<String> out： 收集输出结果，发送到下游
                        */
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        out.collect(MyUtil.parseIterable(elements).toString());
                    }
                }).setParallelism(1)
                .print();
        env.execute();

    }
}
