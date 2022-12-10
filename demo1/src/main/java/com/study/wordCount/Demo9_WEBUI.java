package com.study.wordCount;

import com.study.pojo.WordCount;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-10 16:37
 * @action
 */
public class Demo9_WEBUI {
    public static void main(String[] args) throws Exception {
        //放flink的参数
        //WEBUI: 运行web应用，绑定端口，用于访问
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 3333);

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        /* setRuntimeMode 设置运行模式:
            STREAMING,(默认),
            BATCH,      只适合有界流！
            AUTOMATIC; 根据流的类型选择合适的模式。
                                        有界流： 批处理
                                        无界流： 流处理
        */
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        executionEnvironment.setParallelism(1);
        DataStreamSource<String> dataSource = executionEnvironment.socketTextStream("hadoop102", 8888);
        dataSource.flatMap((String s, Collector<WordCount> collector) -> {
                            for (String s1 : s.split(" ")) {
                                collector.collect(new WordCount(s1, 1));
                            }
                        }
                ).returns(WordCount.class)
                .keyBy(WordCount::getWord)
                .sum("count").print();
        executionEnvironment.execute();
    }
}
