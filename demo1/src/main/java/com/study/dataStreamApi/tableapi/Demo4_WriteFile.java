package com.study.dataStreamApi.tableapi;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhang.siwei
 * @time 2022-12-30 10:40
 * @action  tableapi 将结果写入文件
 */
public class Demo4_WriteFile {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        Table table = tableEnv.fromDataStream(ds);

        //把表的结果写出到某个文件系统
        //先连接上文件系统，注册一张表

        /*
                创建一个可以连接文件系统的描述

                并行度是1，输出到文件中
                并行度>1，输出到目录中  ,
         */
        //创建一个可以连接文件系统的描述
        //将平行度设为1
        FileSystem fileSystem = new FileSystem().path("data/file1");
        //将并行度设为大于1
        //FileSystem fileSystem = new FileSystem().path("data/file");


        //声明表的格式  https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/types/ flink中支持的数据类型
        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        //读文件,制作为表
        /* connect方法过期, Please use {@link #executeSql(String) executeSql(ddl)} to register a table instead.*/
        tableEnv.connect(fileSystem)
                .withFormat(new Csv())//声明文件中数据的格式
                .withSchema(schema) //声明表中的结构
                .createTemporaryTable("t1"); //声明表的名字

        table.executeInsert("t1");
    }
}
