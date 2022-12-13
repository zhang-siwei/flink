package com.study.dataStreamApi.source;

import com.study.function.MySourceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-13 18:22
 * @action  自定义数据源
 */
public class Demo5_CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取自定义的source
        env.addSource(new MySourceFunction()).setParallelism(10).print();
        env.execute();
    }
}
