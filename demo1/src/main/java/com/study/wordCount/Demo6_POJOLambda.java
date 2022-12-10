package com.study.wordCount;

import com.study.pojo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-10 11:31
 * @action 无界流, pojo, lambda
 * <p>
 * *      lamda表达式用于构造函数式接口的实例。
 * *          flink中定义的Function的子类基本都是函数式接口。
 * *
 * *    ----------------------------------
 * *      非法类型异常:
 * *      Caused by: org.apache.flink.api.common.functions.InvalidTypesException:
 * *           Collector<WordCount> 里面的泛型参数丢失
 * *              The generic type parameters of 'Collector' are missing.
 * *
 * *              很多情况下，lamda表达式无法针对泛型类型的提取，提供足够的信息
 * *              In many cases lambda methods don't provide enough information for automatic type extraction
 * *              when Java generics are involved.
 * *
 * *              用匿名内部类，不会出现这个问题
 * *              An easy workaround is to use an (anonymous) class instead that implements
 * *              the 'org.apache.flink.api.common.functions.FlatMapFunction' interface.
 * *
 * *              只要用lamda表达式，Collector<WordCount>里面的泛型信息必须明确指定。
 * *              Otherwise the type has to be specified explicitly using type information.
 * *
 * *     ----------------------------------
 * *     Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException:
 * *          The return type of function 'main(Demo6_POJOLamda.java:44)' could not be determined automatically,
 * *          due to type erasure.
 * *
 * *          解决： 在转换的结果后调用 returns(...) 明确告知泛型的类型信息。
 * *          You can give type information hints by using the returns(...) method on the result of the transformation call,
 * *          or by letting your function implement the 'ResultTypeQueryable' interface.
 * *
 * *  -------------------------------------
 * *         Java有泛型擦除。泛型只在编译时生效。
 * *          在运行时，编译好的字节码中不携带泛型信息的。
 * *                 编译时:  Collector<WordCount>
 * *                 编译后:  Collector<Object>
 */
public class Demo6_POJOLambda {
    public static void main(String[] args) throws Exception {
        //1.创建编程环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        //2.读数据,封装为数据模型
        DataStreamSource<String> dataSource = executionEnvironment.socketTextStream("hadoop102", 8888);
        /*3.计算
         * */
        dataSource.flatMap((String s, Collector<WordCount> collector) -> {
                            for (String s1 : s.split(" ")) {
                                collector.collect(new WordCount(s1, 1));
                            }
                        }
                ).returns(WordCount.class)
                .keyBy(WordCount::getWord)
                .sum("count").print();  // sum() 里需要改为字段名
        //5.触发运行
        executionEnvironment.execute();

    }
}
