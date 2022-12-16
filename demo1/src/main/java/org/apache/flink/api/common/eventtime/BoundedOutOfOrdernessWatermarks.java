/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**

        flink-core-1.13.6-source.jar中有 org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks

        在自己的代码中有  org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks

        基于JVM类加载器的双亲委派模型！
            简单来说，如果同一个包中有 重复签名的类，优先用你自己写的！

 outOfOrdernessMillis 目前是 0

 */
@Public
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    /** The maximum timestamp encountered so far. */
    private long maxTimestamp;

    /** The maximum out-of-orderness that this watermark generator assumes. */
    private final long outOfOrdernessMillis;

    /**
     * Creates a new watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    // ------------------------------------------------------------------------

    // 来一条数据就触发
    /*
            maxTimestamp = 默认是  Long.MIN_VALUE
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        //保证时间不会回退
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        System.out.println(event +"来了,更新maxTimestamp:"+maxTimestamp);

        Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
        System.out.println("当前向下游发送水印:"+watermark.getTimestamp());

        output.emitWatermark(watermark);
    }

    // 周期性发送
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

        Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
//         System.out.println(Thread.currentThread().getName() +"当前向下游发送水印:"+watermark.getTimestamp());
        output.emitWatermark(watermark);
    }
}
