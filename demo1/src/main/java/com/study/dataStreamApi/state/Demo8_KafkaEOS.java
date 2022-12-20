package com.study.dataStreamApi.state;

import com.alibaba.fastjson.JSON;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author zhang.siwei
 * @time 2022-12-20 9:56
 * @action  Kafka+Flink+Kafka 实现端到端严格一次
 * HDFS(采集) ------>Hive导入计算
 * Kafka(采集) ----->flink计算
 * ODS：   kafka---->flink ---->kafka
 * DWD:    kafka---->flink ---->kafka|外部数据库
 * <p>
 * -----------------------------------
 * <p>
 * 保证精确一次。
 * <p>
 * source： 支持重读。 KafkaSource。
 * 在开启CK之后，offsets是作为Source对状态进行保存！
 * 当程序挂掉，恢复重启后，从state中读取上一次保存对offsets，从offsets向后消费，
 * 和kafka中_consumer_offsets中保存对offsets无关！
 * <p>
 * transform:  开启ck
 * <p>
 * sink:    kafkasink(基于2PC实现)
 * ---------------------------------------
 * org.apache.kafka.common.KafkaException:
 * // 生产者出问题
 * Unexpected error in InitProducerIdResponse;
 * //生产者端设置对 事务超时时间 超过了 broker端允许的最大值
 * The transaction timeout is larger than the maximum value allowed by the brok
 * as configured by transaction.max.timeout.ms).
 * broker端默认：  transaction.max.timeout.ms  =  15min
 * FlinkKafkaProducer生产者端: transaction.timeout.ms =  1h
 * 两种选择:  ①改broker   transaction.max.timeout.ms > 1h
 * ②改  FlinkKafkaProducer生产者端，把  transaction.timeout.ms < 15min
 */
public class Demo8_KafkaEOS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启ck
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("topicA")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //设置了自动提交,但是是不会使用提交的offset,直接使用状态中的offset
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
                //设置隔离级别,保障读取的是已经提交的数据
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        Properties properties = new Properties();
        //ProducerConfig: 存放kakfa的生产者的各种参数
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty("transaction.timeout.ms", 10 * 60 * 1000 + "");

        FlinkKafkaProducer<WaterSensor> flinkKafkaProducer = new FlinkKafkaProducer<WaterSensor>(
                "无",
                new KafkaSerializationSchema<WaterSensor>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor waterSensor, @Nullable Long timestamp) {
                        byte[] key = waterSensor.getId().getBytes(StandardCharsets.UTF_8);
                        byte[] value = JSON.toJSONString(waterSensor).getBytes(StandardCharsets.UTF_8);
                        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topicC", key, value);
                        return producerRecord;
                    }
                }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource")
                .map(new WaterSensorMapFunction());
        ds.addSink(flinkKafkaProducer);
        ds.addSink(new SinkFunction<WaterSensor>() {
            @Override
            public void invoke(WaterSensor value, Context context) throws Exception {
                if("s5".equals(value.getId())){
                    throw new RuntimeException("出异常了....");
                }
                System.out.println(value);
            }
        });
        env.execute();
    }
}
