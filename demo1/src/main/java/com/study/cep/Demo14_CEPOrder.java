package com.study.cep;

import com.study.actual.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-29 17:06
 * @action
 */
public class Demo14_CEPOrder {
}

/**
 * Created by Smexy on 2022/12/17
 *
 * 需求: 找到超时(30min)和对账失败(只有支付信息没有订单信息)的订单。
 *
 *  使用下单和支付进行比对。
 *
 *          ①下单 ----支付 ， 间隔(gap)30min以内 ，正常
 *          ②下单 ----支付 ， 间隔30min以外 ，不正常
 *          ③下单                          正常
 *          ④支付                          不正常
 *
 *
 *     按照orderId分组，把同一笔单的支付和下单分到一起。
 *
 *  ---------------------------------
 *      定义规则检测:
 *          ②下单 ----支付 ，          间隔30min以外 ，不正常
 *          ④         支付                          不正常
 *
 *          可以使用
 *                 要点1: begin(下单).optional.next(支付)
 *
 *        ---------
 *          如果有以下数据:   下单 ----支付 ， 间隔(gap)30min以内 ，正常。
 *              此时  begin(下单).optional.next(支付)，同一条数据会匹配2次。
 *                  要点2:    通过 跳过策略，将部分对过滤掉！
 *
 *        -----------
 *          需要添加时效范围within，将超时和未超时的数据区分！
 *                 要点3: begin(下单).within(30min).optional.next(支付)
 *                      匹配成功：   ①下单 ----支付 ， 间隔(gap)30min以内 ，正常
 *
 *                      是②下单 ----支付 ， 间隔30min以外 ，不正常，此时称为下单超时
 *
 *
 *
 */
/*public class Demo14_CEPOrder
{
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner( (e, r) -> e.getEventTime() * 1000);

        env.setParallelism(1);

        KeyedStream<OrderEvent, Long> ds = env.readTextFile("data/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>()
                {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(words[0]),
                                words[1],
                                words[2],
                                Long.valueOf(words[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(OrderEvent::getOrderId);

        //定义规则
        // 减少匹配。
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("order", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<OrderEvent>()
                {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                //目的将 超时支付和未超时支付对情况区分。  未超时支付，正常匹配。超时支付，可以在超时数据中找到create的信息
                .within(Time.minutes(30))
                //目的是匹配  只有pay的情况
                .optional()
                .next("pay")
                .where(new SimpleCondition<OrderEvent>()
                {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                });

        SingleOutputStreamOperator<OrderEvent> result = CEP.pattern(ds, pattern)
                .process(new MyProcess());



        Pattern<OrderEvent, OrderEvent> pattern2 = Pattern.<OrderEvent>begin(
                "order", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<OrderEvent>()
                {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                //目的是匹配  只有pay的情况
                .optional()
                .next("pay")
                .where(new SimpleCondition<OrderEvent>()
                {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                });

        *//*
          --异常
            OrderEvent(orderId=2, eventType=create, txId=abc, eventTime=1558432021)
            OrderEvent(orderId=2, eventType=pay, txId=abc, eventTime=1568432021)

            OrderEvent(orderId=1, eventType=pay, txId=abc, eventTime=1558432021)
            -- 正常
            OrderEvent(orderId=34768, eventType=create, txId=88snrn932, eventTime=1558430950)
         *//*
        KeyedStream<OrderEvent, Long> exResult = result.getSideOutput(new OutputTag<OrderEvent>("timeout", TypeInformation.of(OrderEvent.class)))
                .keyBy(OrderEvent::getOrderId);

        CEP.pattern(exResult,pattern2)
                .select(new PatternSelectFunction<OrderEvent, String>()
                {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {

                        Long orderId = pattern.get("pay").get(0).getOrderId();

                        if (pattern.containsKey("order")){
                            //超时
                            return orderId + "支付超时";

                        }else{
                            return orderId + "缺少下单create";
                        }


                    }
                })
                .print();


        env.execute();

    }

    *//*
            ①下单 ----支付 ， 间隔(gap)30min以内 ，正常。  不超时，匹配成功
 *          ②下单 ----支付 ， 间隔30min以外 ，不正常       超时，下单超时
 *          ③下单                          正常
                数据: orderid = 1,ordertime = 15：26
                水印已经推进到 19：00, orderid=1 ,后续的数据还没来

 *          ④支付                          不正常
     *//*
    private static class MyProcess  extends PatternProcessFunction<OrderEvent, OrderEvent> implements TimedOutPartialMatchHandler<OrderEvent>
    {

        *//*
            处理未超时，匹配到的数据
                    ①下单 ----支付 ， 间隔(gap)30min以内
                                                        获取create和pay
                    ④支付                          不正常
                                                        获取pay


             AfterMatchSkipStrategy.skipPastLastEvent():  匹配成功后，从多个成功的匹配条件中，减少输出！
                    ②下单 ----支付 ， 间隔30min以外 ，不正常       超时，下单超时
                            create.within(30min).next(pay) 条件。  超时，未匹配
                                                       pay 条件。  可以匹配

                                                       获取pay



         *//*
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, PatternProcessFunction.Context ctx, Collector<OrderEvent> out) throws Exception {
            //找只有支付的数据
            if (!match.containsKey("order")){
                //把只有pay的数据输出到侧流和超时对数据汇合，进一步判断    会输出 ④的pay 和 ②对pay
                ctx.output(new OutputTag<OrderEvent>("timeout", TypeInformation.of(OrderEvent.class)),match.get("pay").get(0));

            }

        }

        *//*
            处理超时的数据

                 ②下单 ----支付 ， 间隔30min以外 ，不正常       超时，下单超时
                        只有下单

                 ③下单                          正常

                 无法判断出当前这笔订单，到底是未支付，还是支付超时！结合processMatch输出对支付信息，进一步判断。
         *//*
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            //输出到侧流
            ctx.output(new OutputTag<OrderEvent>("timeout", TypeInformation.of(OrderEvent.class)),match.get("order").get(0));

        }
    }
}*/
