package com.study.dataStreamApi.transform;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:42
 * @action 算子 reduce 练习
 *  *  reduce:
 *  *              用于两两聚合。第一条数据来不执行！
 *  *              要求输入和输出的类型必须一致
 *  *                  (T t1,T t2) ---> T t3
 *  *
 *  *    ------------
 *  *    求每种传感器vc的最大值
 */
public class Demo8_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    /*
                             WaterSensor value1：上一次聚合的结果
                             WaterSensor value2： 当前到来的数据
                        */
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        //取最大的vc + 最新的WaterSensor的所有属性
                        value2.setVc(Math.max(value1.getVc(), value2.getVc()));
                        return value2;
                    }
                })
                .print();
        env.execute();
    }
}
