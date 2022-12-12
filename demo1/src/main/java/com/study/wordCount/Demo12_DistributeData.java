package com.study.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 *      上下游算子直接的分发规则。
 *              ChannelSelector:  决定分发规则
 *                  int selectChannel(T var1)：  决定var1,要发送到下游的哪个通道
 *
 *                  StreamPartitioner：  将元素，根据分区规则，从上游发送到下游
 *
 *      使用分区器，只需要调用对应的算子即可。
 *                    .rescale()   RescalePartitioner：    单个TaskManger内，负载均衡。极端情况下，也可能出现数据倾斜
 *                    .rebalance()   RebalancePartitioner： 全局，负载均衡。 默认。
 *
 *                    .keyBy()  KeyGroupStreamPartitioner（hash分区）： 按照key hash分区.
 *                                      key相同，发往同一个下游的Task处理！
 *                                      key不同，有可能发往同一个下游的Task处理！
 *                    .global()  GlobalPartitioner：     全局汇总，上游的数据，默认发送到下游的第一个通道！
 *
 *                    .forward()  ForwardPartitioner：   上下游1对1发送。
 *                                                          上下游算子的并行度必须是一致的！
 *                                                          不能是1对多，也不能是多对1
 *
 *                                                          只要两个算子是 forward，默认就会OpetatorChain。
 *
 *                    .shuffle()  ShufflePartitioner：   随机分发。上游的数据，随机选择一个下游的通道
 *                     自定义单播 CustomPartitionerWrapper： 自己编写继承CustomPartitionerWrapper，
 *                                                                  单播： 一个元素，只能返回一个channel
 *                    .broadcast()  BroadcastPartitioner：  广播，上游的每一条数据发往下游的每一个通道。
 *
 *
 *
 *
 */
public class Demo12_DistributeData
{
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.setInteger("rest.port",3333);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //全局禁用 OperatorChain
        env.disableOperatorChaining();

       env.socketTextStream("hadoop103",8888)
          .map(line -> line).setParallelism(3)
          .forward()
          .print().setParallelism(3);


        env.execute();

    }
}
