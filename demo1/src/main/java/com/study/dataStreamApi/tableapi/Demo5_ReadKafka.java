package com.study.dataStreamApi.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhang.siwei
 * @time 2022-12-30 11:18
 * @action  读取kafka数据
 */
public class Demo5_ReadKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Kafka kafka = new Kafka()
                .topic("topicD")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "test1")
                .property(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .property(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"500")
                //这个组从来没消费过,默认latest
                .startFromGroupOffsets()
                /* 不加version会下列报错,百度原因是缺少依赖
                Could not find a suitable table factory for 'org.apache.flink.table.factories.TableSourceFactory' in
                    the classpath.
                */
                .version("universal")
                ;
        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        //读kafka,制作为表
        tableEnv.connect(kafka)
                .withFormat(new Json())  //声明kafka中数据的格式
                .withSchema(schema)   //声明表结构
                .createTemporaryTable("t1");  //声明表的名字
        //查询
        Table table = tableEnv.from("t1");

        table.select($("id"),$("ts"),$("vc"))
                .execute()
                .print();
    }
}
