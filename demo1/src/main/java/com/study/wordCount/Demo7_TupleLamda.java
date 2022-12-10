package com.study.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.Types.*;

/**
 * @author zhang.siwei
 * @time 2022-12-10 16:23
 * @action tuple,lambda
 *
 * 静态导入：把其他类中的静态属性或方法，通过静态导入的方式导入到当前类中，好比当前类拥有了静态的属性和方法，直接调用
 * returns(Class c): 只针对定义的POJO类型
 *          如果是Tuple，无效的！可以使用
 *
 *  returns(TypeHint t)
 *      或
 *    returns(TypeInformation<T> typeInfo)
 */
public class Demo7_TupleLamda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<String> data = environment.socketTextStream("hadoop102", 8888);
        data.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    for (String s1 : s.split(" ")) {
                        collector.collect(new Tuple2<>(s1, 1));
                    }
                })
                .returns(TUPLE(STRING,INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        environment.execute();
    }
}
