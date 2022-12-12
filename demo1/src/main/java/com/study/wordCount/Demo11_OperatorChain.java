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
 *      自动开启。
 *          OperatorChain条件：
 *                  上下游是 forward传输：   上游和下游，并行度要相同
 *                                          必须是1:1
 *                                                  不能是1:N  : shuffle,rescale,reblance,broadcast,keyBy
 *                                                  不能是N：1 : global
 *
 *                  有相同的SlotSharingGroup的算子才能OperatorChain。
 *                      默认情况，没有设置时，所有算子的SlotSharingGroup都是相同的。
 *
 *
 *                      SlotSharingGroup： 标记，组名。为了将资源消耗不同的算子进行区分。
 *
 *      --------------------
 *          资源充足，希望每个算子都有自己的Slot，不和其他的算子进行 OperatorChain
 *               全局禁用
 *
 */
public class Demo11_OperatorChain
{
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",3333);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //env.disableOperatorChaining();

        env.socketTextStream("hadoop103",8888)
           .map(line -> line).setParallelism(3).slotSharingGroup("s1")
           .map(line -> line).setParallelism(3)
           //.startNewChain():从这个算子开始，另起炉灶。和前面的算子不链在一起
           //.disableChaining:这个算子开始，和前后的算子不链在一起
           .map(line -> line).name("断开").setParallelism(3).disableChaining()//.startNewChain()
           .map(line -> line).setParallelism(3)
           .print().setParallelism(3).slotSharingGroup("s2");


        env.execute();


    }
}
