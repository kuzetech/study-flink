package com.kuze.bigdata.study.watermark;


import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple2;

public class TimeStampExtrator implements TimestampAssigner<Tuple2<String, Long>> {

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
        return element.f1;
    }
}
