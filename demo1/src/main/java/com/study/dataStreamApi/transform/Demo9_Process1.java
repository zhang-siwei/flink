package com.study.dataStreamApi.transform;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-13 19:49
 * @action 算子 process 练习
 * *  process:
 *  *          最底层算子。 强大，灵活，需要编程的多！
 *  *   -----------------------------
 *  *      希望统计所有传感器(不分类型)的vc和
 */
public class Demo9_Process1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                //.global()
                .process(new ProcessFunction<WaterSensor, String>() {
                    //此处定义的是属性
                    int sum =0;
                    /*
                            WaterSensor value: 当前的数据
                            Context ctx：  上下文(编程环境)，从这里获取一些额外的信息
                            Collector<String> out： 收集输出，发送到下游
                        */
                    @Override
                    public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        sum+=waterSensor.getVc();
                        collector.collect("总和: "+sum);
                    }
                }).setParallelism(1)
                .print();
        env.execute();
    }
}
