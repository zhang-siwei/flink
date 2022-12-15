package com.study.dataStreamApi.window;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-15 17:07
 * @action  带keyby的基于元素的窗口
 * 性质:基于元素个数的窗口
 *             count
 * 计算： KeyBy
 *             xxx。
 *             每个key
 * 功能:  滑动
 */
public class Demo2_CountWindowKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                //.countWindowAll(3)  //滚动
                //.countWindowAll(3,2)  //滑动,滑的小 ,重复计算
                .countWindowAll(3,4) //滑动,滑的大,漏算
                .process(new ProcessAllWindowFunction<WaterSensor, String, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        out.collect(MyUtil.parseIterable(elements).toString());
                    }
                }).setParallelism(1)
                .print();
        env.execute();
    }
}
