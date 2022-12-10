package com.study.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-10 11:31
 * @action 有界流
 */
public class Demo2_BoudedStreamExection {
    public static void main(String[] args) throws Exception {
        //1.创建编程环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //全局只让一个Task运算
        executionEnvironment.setParallelism(1);
        //2.读数据,封装为数据模型,默认读取utf-8编码文件
        DataStreamSource<String> dataSource = executionEnvironment.readTextFile("data/word.txt");
        /*3.计算
        * 接口的实现有三种:
                    内部实现类
                    外部实现类
                    匿名内部实现类( 函数式接口 lamda)

               Tuple2:  ck中 tuple(第一个元素key,第二个元素value)
               FlatMapOperator: 封装了对流或批的计算逻辑
        * */
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
        });
        /*
                对Tuple2 groupBy，需要调用 groupBy(int position)
                        position: 要分组的字段在tuple中的位置。

                对POJO(Bean)按照某个属性进行分组，调用 groupBy(String fieldName)
         */
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
}
