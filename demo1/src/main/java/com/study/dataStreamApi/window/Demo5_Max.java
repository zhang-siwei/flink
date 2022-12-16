package com.study.dataStreamApi.window;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-16 13:56
 * @action 求过去每3个传感器中，vc最大的传感器
 *     常见的聚合操作，无keyBy，不运算
 */
public class Demo5_Max {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(w -> "w")
                .countWindow(3)
                .max("vc")
                .print();
        env.execute();
    }
}
