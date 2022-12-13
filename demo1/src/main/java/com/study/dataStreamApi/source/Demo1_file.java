package com.study.dataStreamApi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-13 11:10
 * @action  从文件或目录读取数据
 *  * UnsupportedFileSystemSchemeException: Hadoop is not in the classpath/dependencies.
 */
public class Demo1_file {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读文件时，可以指定文件的目录，也可以精确到文件
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/input");
        streamSource.print();
        env.execute();

    }
}
