package com.kuze.bigdata.study.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WatermarkWindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = env.addSource(new GenerateSource())
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] split = value.split(",");
                        out.collect(Tuple2.of(split[0], Long.valueOf(split[1])));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new TimeStampExtrator())
                ).keyBy(tuple -> tuple.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //.allowedLateness(Time.seconds(5)) //再延迟5秒
                .sideOutputLateData(outputTag)
                .process(new SumProcessFunction());

        result.print();

        DataStream<Tuple2<String, Long>> lateStream = result.getSideOutput(outputTag);

        lateStream.map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> value) throws Exception {
                return "迟到的数据：" + value.toString();
            }
        }).print();

        env.execute("window word count");
    }

    public static class SumProcessFunction extends ProcessWindowFunction<
            Tuple2<String, Long>, Tuple2<String, Integer>, String, TimeWindow>{
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple2<String, Long>> elements,
                Collector<Tuple2<String, Integer>> out) throws Exception {

            System.out.println("当前处理时间为：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间为：" + dateFormat.format(context.window().getStart()));
            System.out.println("窗口结束时间为：" + dateFormat.format(context.window().getEnd()));

            int count = 0;
            for(Tuple2<String, Long> e : elements){
                count ++;
            }
            out.collect(Tuple2.of(key, count));
        }
    }
}
