package com.study.wordCount;

import com.study.pojo.WordCount;
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
 * @action 无界流,pojo
 *         缺点： 麻烦，需要自己编写POJO
 *  *      优点： 字段有了元数据说明。
 *  *                  Tuple2(f0,f1): 只能通过位置声明，字段没有意义。
 *  *                  POJO: 每个属性都有属性名
 *  *
 *  *             Tuple2： 能封装的数据个数是有限的。
 *  *                      只能封装两个属性。
 *  *                      如果数据字段过多，最多只能封装到Tuple25。
 */
public class Demo5_POJOImpl {
    public static void main(String[] args) throws Exception {
        //1.创建编程环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        //2.读数据,封装为数据模型
        DataStreamSource<String> dataSource = executionEnvironment.socketTextStream("hadoop102", 8888);
        /*3.计算
        * */
        SingleOutputStreamOperator<WordCount> operator = dataSource.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                for (String s1 : s.split(" ")) {
                    collector.collect(new WordCount(s1, 1));
                }
            }
        });

        operator.keyBy(new KeySelector<WordCount, String>() {
            @Override
            public String getKey(WordCount wordCount) throws Exception {
                return wordCount.getWord();
            }
        }).sum("count").print();  // sum() 里需要改为字段名
        //5.触发运行
        executionEnvironment.execute();

    }
}
