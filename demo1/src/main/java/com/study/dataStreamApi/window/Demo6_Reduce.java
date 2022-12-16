package com.study.dataStreamApi.window;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-16 14:05
 * @action 求过去每3个传感器中，vc的和
 *     reduce： 两两聚合
 *     按照聚合方式来分类:
 *         滚动聚合（和不开窗的聚合方式一样）:  来一条数据，就聚合一次。
 *                     reduce,aggretate,sum,max,maxBy,min,minBy
 *                     特例: 基于元素个数的滑动窗口，以上算子变为非滚动聚合。
 *         非滚动聚合:   process,apply
 *                         只有触发窗口运算时，才会进行计算。
 *                         把窗口的元素攒齐了，才计算。
 *     容易混淆:       process 不开窗计算，是滚动聚合
 *                     开窗后使用process，是非滚动聚合
 */
public class Demo6_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(w -> "w")
                // reduce是滚动聚合
                //.countWindow(3)
                .countWindow(5,3)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor v1, WaterSensor v2) throws Exception {
                        System.out.println("Demo6_Reduce "+v1+"  "+v2);
                        v2.setVc(v1.getVc()+v2.getVc());
                        return v2;
                    }
                })
                .print();
        env.execute();
    }
}
