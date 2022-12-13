package com.study.dataStreamApi.transform;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:42
 * @action 聚合 算子 练习
 * *  在sql中，不group by是无法调用聚合函数。
 *  *
 *  *  在代码中，不分组(keyBy)，无法调用聚合的算子。
 *  *
 *  *  ------------------
 *  *      按照传感器的id，统计不同种类传感器的vc和
 *  *          sum:  对指定的字段进行累加，其他字段取的都是组内第一条数据的属性。
 *  *          max|min:  对指定的字段取最大和最小值，其他字段取的都是组内最大或最小数据的属性。
 *  *          maxBy|minBy:  对指定的字段取最大和最小值，所有字段取的都是组内最大或最小数据的属性。
 *  *                  minBy(字段)： 遇到两条同样是最小的，依旧取第一条的所有属性
 */
public class Demo7_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                //.sum("vc")
                //.max("vc")
                .min("vc")
                // 如果遇到两条数据的vc一样，默认不取第一条，取最后一条
                //.maxBy("vc", false)
                //.minBy("vc", false)
                .print();
        env.execute();
    }
}
