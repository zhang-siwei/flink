package com.study.dataStreamApi.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:42
 * @action 算子 RichMap 练习
 *  *      需要在进行map时，去连接数据库。
 *  *
 *  *      希望有些操作只在map算子计算时，执行一次，例如开启连接，关闭连接。
 *  *              不能用普通的XXXFunction，而需要使用 RichXxxFunction
 *  *                      富有在于多了两个生命周期方法:(每个sourcetask都会执行,即一个并行度执行一次(没被使用的task也会执行))
 *  *                              open():  Task出生时执行
 *  *                              close(): Task死亡时执行
 *  *
 *  *
 *  *      ------------------------
 *  *      RichMapFunction<IN, OUT> extends AbstractRichFunction implements MapFunction<IN, OUT>
 *  *                  既有 MapFunction的功能，还有 AbstractRichFunction 所提供的生命周期方法
 *  *
 *  *      -------------------
 *  *          大部分的算子都提供了 RichXxxFuction
 */
public class Demo2_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        streamSource.map(new RichMapFunction<Integer, String>() {
            String conn;
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("获取数据库连接....");
                conn="lainjie";
            }

            @Override
            public String map(Integer integer) throws Exception {
                System.out.println("使用数据库连接");
                return "num: "+integer;
            }

            @Override
            public void close() throws Exception {
                System.out.println("关闭数据库连接...");
                conn=null;
            }

        }).print();
        env.execute();
    }
}
