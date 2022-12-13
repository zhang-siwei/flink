package com.study.function;

import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zhang.siwei
 * @time 2022-12-13 19:18
 * @action
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String s) throws Exception {
        String[] s1 = s.split(" ");
        return new WaterSensor(s1[0], Long.valueOf(s1[1]), Integer.valueOf(s1[2]));
    }
}
