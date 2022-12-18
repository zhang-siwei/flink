package com.study.function;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-15 16:55
 * @action
 */
public class MyUtil {
    private static SimpleDateFormat simpleDateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getTimeWindow(TimeWindow t){
        return "["+simpleDateFormat.format(new Date(t.getStart()))
                +" , "+
                simpleDateFormat.format(new Date(t.getEnd()))+"]";
    }
    public static void printTimeWindow(TimeWindow t) {
        System.out.println(
                simpleDateFormat.format(new Date(t.getStart()))
                        +" , "+
                        simpleDateFormat.format(new Date(t.getEnd()))
        );
    }

    public static <T>List<T> parseIterable(Iterable<T> elements){
        List<T> result = new ArrayList<>();
        for (T element : elements) {
            result.add(element);
        }
        return result;
    }
}
