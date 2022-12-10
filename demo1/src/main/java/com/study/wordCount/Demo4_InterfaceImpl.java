package com.study.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-10 11:31
 * @action 无界流： 内部类实现
 *  *      模拟无界流：    sudo yum -y install nc
 *  *          绑定服务端: nc -lk 主机名 端口
 *  *          连接服务端: nc 主机名 端口 (了解)，使用flink去连接
 */
public class Demo4_InterfaceImpl {
    public static void main(String[] args) throws Exception {
        //1.创建编程环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //全局只让一个Task运算
        executionEnvironment.setParallelism(1);
        //2.读数据,封装为数据模型
        DataStreamSource<String> dataSource = executionEnvironment.socketTextStream("hadoop102", 8888);
        /*3.计算
        * */
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = dataSource.flatMap(new MyFuncation());

        operator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            //把输入的每个元素  Tuple2<String, Integer>的哪部分作为key输出
            @Override
            public String getKey(Tuple2<String, Integer> tup) throws Exception {
                return tup.f0;
            }
        }).sum(1).print();
        //5.触发运行
        executionEnvironment.execute();
    }

    public static class MyFuncation implements FlatMapFunction<String,Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            /*
                        每一行按照 空格切分，每个单词都以 (单词，1)写出

                        collector：用于收集要写出的结果。
                   */
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        }
    }
}
