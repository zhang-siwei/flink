package com.study.wordCount;

import com.study.pojo.WordCount;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-10 16:33
 * @action  流,批一体
 * 正常使用流处理，获取实时结果。
 *  *  在当天数据全部生成后，调用批处理进行对数，补数。
 *  *
 *  *  flink在1.12推出了流批一体。
 *  *              一套代码，只需要进行简单的配置，就可以用 流|批 模式去运行。
 *  *
 *  *          用的是流处理的API。
 */
public class Demo8_StreamBatchOne {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
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
