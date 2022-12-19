package com.study.function;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-15 16:55
 * @action
 */
public class MyUtil {
    /*JDK8以后推荐的  DateTimeFormatter
        是否在 java.time.包下。
        新的API全部都是静态方法
      DateTimeFormatter  对应  SimpleDateFormat ： 日期格式
      LocalDateTime   对应  Date  ：  日期对象
        构造方式:
            LocalDateTime.ofInstant(Instant.ofEpochMilli(毫秒), ZoneId.of("Asia/Shanghai"));
    * */
    //线程安全的
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static DateTimeFormatter getDateTimeFormatter(){
        return DateTimeFormatter.ofPattern("yyyy-MM-dd");
    }
    public static String getTimeWindow(TimeWindow t) {
        LocalDateTime startDt = LocalDateTime.ofInstant(Instant.ofEpochMilli(t.getStart()), ZoneId.of("Asia/Shanghai"));
        LocalDateTime endDt = LocalDateTime.ofInstant(Instant.ofEpochMilli(t.getEnd()), ZoneId.of("Asia/Shanghai"));
        return "[" + startDt.format(dateTimeFormatter)
                + " , " +
                endDt.format(dateTimeFormatter)
                + "]";
    }

    //有线程安全问题
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //    public static String getTimeWindow(TimeWindow t){
//        return "["+simpleDateFormat.format(new Date(t.getStart()))
//                +" , "+
//                simpleDateFormat.format(new Date(t.getEnd()))+"]";
//    }
    public static void printTimeWindow(TimeWindow t) {
        System.out.println(
                simpleDateFormat.format(new Date(t.getStart()))
                        + " , " +
                        simpleDateFormat.format(new Date(t.getEnd()))
        );
    }

    public static <T> List<T> parseIterable(Iterable<T> elements) {
        List<T> result = new ArrayList<>();
        for (T element : elements) {
            result.add(element);
        }
        return result;
    }
}
