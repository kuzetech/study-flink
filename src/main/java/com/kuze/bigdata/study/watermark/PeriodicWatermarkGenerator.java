package com.kuze.bigdata.study.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

public class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>> {

    private long maxEvenTime = 0;

    @Override
    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
        Long currentElementEvenTime = event.f1;
        maxEvenTime = Math.max(maxEvenTime, currentElementEvenTime);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxEvenTime - 5000));
    }
}
