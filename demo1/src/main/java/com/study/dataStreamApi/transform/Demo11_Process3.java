package com.study.dataStreamApi.transform;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhang.siwei
 * @time 2022-12-13 19:49
 * @action 算子 process 练习
    分流。
 *          把一个流分为多个流。
 *                  1个主流，剩下的都是测流(SideOutput)
 *
 *       把s1类型的传感器，分入主流。
 *       s2到测流
 *       s3到测流
 */
public class Demo11_Process3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //常见的数据类型，直接使用 Types.XXX 获取 TypeInformation
        // String:Types.STRING
        // 自定义的类型，可以使用 TypeInformation.of(Class c)
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", TypeInformation.of(WaterSensor.class));
        OutputTag<WaterSensor> s3 = new OutputTag<>("s3", TypeInformation.of(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> stream = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {
                        String id = waterSensor.getId();
                        if ("s1".equals(id)) {
                            //主流输出
                            collector.collect(waterSensor);
                        } else if ("s2".equals(id)) {
                             /*
                                    侧流输出

                                    output(OutputTag<X> outputTag, X value)
                                            OutputTag<X> outputTag: 一个标记，用于说明测流中输出的数据类型，为测流起一个id
                                             X value； 输出的数据
                                */
                            context.output(s2, waterSensor);
                        } else if ("s3".equals(id)) {
                            context.output(s3, waterSensor);
                        }
                    }
                });
        //选择主流进行操作
        stream.print("主流");
        //获取测流
        stream.getSideOutput(s2).printToErr("s2测流");
        stream.getSideOutput(s3).printToErr("s3测流");
        env.execute();
    }
}
