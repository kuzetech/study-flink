package com.kuze.bigdata.study.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class GenerateSource implements SourceFunction<String> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //确保程序在10秒整开始运作
        String currentTime = String.valueOf(System.currentTimeMillis());
        while (Integer.valueOf(currentTime.substring(currentTime.length() - 4)) > 100){
            currentTime = String.valueOf(System.currentTimeMillis());
            continue;
        }

        System.out.println("当前时间为：" + dateFormat.format(System.currentTimeMillis()));
        Thread.sleep(3000);
        long time = System.currentTimeMillis();
        ctx.collect("flink," + time);
        Thread.sleep(3000);
        ctx.collect("flink," + System.currentTimeMillis());
        Thread.sleep(6000);
        ctx.collect("flink," + time);
        Thread.sleep(10000);
        ctx.collect("flink," + System.currentTimeMillis());

        Thread.sleep(1000000000);
    }

    @Override
    public void cancel() {

    }
}
