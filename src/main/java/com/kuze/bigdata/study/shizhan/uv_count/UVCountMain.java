package com.kuze.bigdata.study.shizhan.uv_count;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UVCountMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        /*env
                .readTextFile("/Users/huangsw/code/study/study-flink/src/main/resources/data/uv_count.log")
                .map(o->UVCountLog.parseLog(o))
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGem))*/

        env.execute("dau");

    }

}
