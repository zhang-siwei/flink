package com.study.dataStreamApi.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhang.siwei
 * @time 2022-12-30 10:40
 * @action  tableapi 读取文件
 */
public class Demo3_ReadFile {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建一个可以连接文件系统的描述
        FileSystem fileSystem = new FileSystem().path("data/sensor.txt");

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
        //查询
        Table table = tableEnv.from("t1");

        table.select($("id"),$("ts"),$("vc"))
                .execute()
                .print();
    }
}
