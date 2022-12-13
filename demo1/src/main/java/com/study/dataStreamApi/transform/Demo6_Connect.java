package com.study.dataStreamApi.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:42
 * @action 算子 connect 练习
 * Connect连接，用于处理不同类型的流，进行混合。
 *  *
 *  *       猪---->猪肉(90%)                    -------->  猪肉+ 牛肉精
 *  *                          -----> 肉馅机器                            -----> 输出 混合馅料  牛肉馅
 *  *       牛---->牛肉(10%)                    -------->   牛肉 + 香精
 *  *
 *  *   -------------------------------------
 *  *
 *  *      泾渭分明
 *  *
 *  *          泾河龙王 ---->吐口水 ----->泾河----> 清                            ----> 清
 *  *
 *  *                                                   ------>汇入 ---------                 -----> 黄河
 *  *          渭河龙王---->吐口水 ----->泾河-----> 浑                            -----> 浑
 */
public class Demo6_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> streamSource1 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> streamSource4 = env.fromElements("1","2");
        //CoMapFunction<Integer, String, Long> : 入参类型1,入参类型2,出参类型 ,分别处理两个入参类型
        streamSource1.connect(streamSource4).map(new CoMapFunction<Integer, String, Long>() {
            @Override
            public Long map1(Integer integer) throws Exception {
                return integer+0L;
            }

            @Override
            public Long map2(String s) throws Exception {
                return Long.valueOf(s);
            }
        }).print();
        env.execute();
    }
}
