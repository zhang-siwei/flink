package com.study.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**  并行度练习
 */
public class Demo10_Parilisim
{
    public static void main(String[] args) throws Exception {

        //1.获取编程环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",3333);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //全局只让一个Task运算
        env.setParallelism(1);

        //2.读数据，获取编程模型
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop103",8888).setParallelism(1);

        //3.进行处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>()
        {
            @Override
            public void flatMap(String inputLine, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = inputLine.split(" ");

                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> o2 = operator.setParallelism(4);

        //4.分组
        KeyedStream<Tuple2<String, Integer>, String> ks = o2.keyBy(new KeySelector<Tuple2<String, Integer>, String>()
        {
            //把输入的每个元素  Tuple2<String, Integer>的哪部分作为key输出
            @Override
            public String getKey(Tuple2<String, Integer> ele) throws Exception {
                return ele.f0;
            }
        });


        ks
          .sum(1).setParallelism(3)
          .print().setParallelism(2);


        //5.触发运行
        env.execute();

    }
}
