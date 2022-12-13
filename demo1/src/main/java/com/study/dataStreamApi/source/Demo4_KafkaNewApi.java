package com.study.dataStreamApi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author zhang.siwei
 * @time 2022-12-13 11:34
 * @action  flink 1.4 之后连接kafka
 *  *  1.14之后推荐
 *  *      所有的sourceAPI，统一为 data source
 *  *          env.fromSource(DataSource d)
 *  *
 *  *   -----------------------
 *  *          以下声明:
 *  *          public static <OUT> KafkaSourceBuilder<OUT> builder()
 *  *              调用的格式:   <OUT>方法名(参数列表)
 *  *
 *  *   ---------------------------
 *  *      启动消费者时，flink其实并不是从Kafka保存的offsets中获取偏移量，向后消费！
 *  *
 *  *      而是从状态中获取offsets！
 *  *              当状态中，也没有offsets时，才会参考Kafka保存的offsets中获取偏移量，向后消费。
 */
public class Demo4_KafkaNewApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //构造Source
        /*
            <String>builder():
                    消费的ConsumerRecord中value的类型是String。

                    K-V：
                            K只在生产者有用，用于分区。
                            消费者，一般不获取K
         */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("topicA")
                .setGroupId("flink")
                /*
                从哪个位置消费
                    从最早的位置:  OffsetsInitializer.earliest()
                    从最后的位置: OffsetsInitializer.latest()
                    从消费者组消费的位置:
                            OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
                                    如果组是第一次消费，默认从最后位置消费
                    从指定的位置:  OffsetsInitializer.offsets(Map m)
             */
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
                .build();
        /*
                添加Source

                fromSource(
                    Source<OUT, ?, ?> source 对象,
                  WatermarkStrategy<OUT> timestampsAndWatermarks， 水印(后续)策略。从源头生成水印。
                        WatermarkStrategy.noWatermarks()： 当前不生成水印
                String sourceName：  为source起个名字，这个名字可以在监控界面看到。
                 )
             */
        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        streamSource.print();
        env.execute();

    }
}
