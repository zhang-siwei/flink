package com.study.dataStreamApi.tableapi;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author zhang.siwei
 * @time 2022-12-30 15:30
 * @action  写入kafka数据
 */
public class Demo6_WriteKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());
        Table table = tableEnv.fromDataStream(ds);

        //注册向kafka写入的连接器
        Kafka kafka = new Kafka()
                .topic("topicD")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                //轮询写
                //.sinkPartitionerRoundRobin()
                //尽量将是一个sink的task向一个固定的分区写,如果kafka的分区少,sink的分区多,出现多个task向1个分区写
                .sinkPartitionerFixed()
                //必须加
                .version("universal")
                ;
        //声明表格式
        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        //读kafka,制作为表
        tableEnv.connect(kafka)
                .withFormat(new Json())  //声明kafka中数据的格式
                .withSchema(schema)   //声明表结构
                .createTemporaryTable("t1");  //声明表的名字
        //执行写出
        table.executeInsert("t1");
    }
}
