package com.study.actual;

import com.study.actual.pojo.OrderEvent;
import com.study.actual.pojo.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author zhang.siwei
 * @time 2022-12-15 16:10
 * @action 来自两条流的订单交易匹配
 * <p>
 * 1.读两种不同的数据，获取两种流
 * 2.两个流 必须 connect才能对账
 * 3.connect之后，保证同一个 支付id(txId)的两种数据可以发到下游的同一个Task
 * keyBy(txId)
 * 4.connect之后再进行处理，必须调用 coXxx 算子
 * coXxx 算子在处理时，依旧提供两个方法，处理各自的流。
 * 每个方法中的变量无法直接访问，必须借助 成员变量(属性)。
 * 提供两个属性:
 * 用于缓存OrderEvent
 * 用于缓存 TxEvent
 * 如何判断是否对账成功：
 * 两种流的数据在处理时，读对应的缓存，读到就说明对账成功，输出。
 * 读不到，就把自己写入自己的缓存，等待对方类型在处理时，再判断是否可以对账成功。
 */
public class Actual5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<OrderEvent> op1 = env.readTextFile("data/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new OrderEvent(Long.valueOf(split[0]),
                                split[1], split[2], Long.valueOf(split[3]));
                    }
                }).filter(o -> "pay".equals(o.getEventType()));
        SingleOutputStreamOperator<TxEvent> op2 = env.readTextFile("data/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new TxEvent(split[0],
                                split[1], Long.valueOf(split[2]));
                    }
                });

        op1.connect(op2)
                .keyBy(or -> or.getTxId(), tx -> tx.getTxId())
                .map(new CoMapFunction<OrderEvent, TxEvent, Long>() {
                    //缓存OrderEvent，需要缓存txId,还要缓存orderId
                    Map<String, OrderEvent> or = new HashMap<>();
                    //缓存TxEvent，只需要存txId
                    Set<String> tx = new HashSet<>();

                    @Override
                    public Long map1(OrderEvent value) throws Exception {
                        Long s = 0L;
                        //处理当前的OrderEvent，需要访问 TxEvent的缓存，判断是否对账成功
                        if (tx.contains(value.getTxId())) {
                            s = value.getOrderId();
                        } else {
                            //要对账的TxEvent还没到达task(要写入缓存，等待)，或压根不存在
                            or.put(value.getTxId(), value);
                        }
                        return s;
                    }

                    @Override
                    public Long map2(TxEvent value) throws Exception {
                        Long s = 0L;
                        if (or.containsKey(value.getTxId())) {
                            s = or.get(value.getTxId()).getOrderId();
                        } else {
                            tx.add(value.getTxId());
                        }
                        return s;
                    }
                }).filter(s -> s != 0)
                .print().setParallelism(1);
        env.execute();
    }
}
