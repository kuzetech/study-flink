package com.kuze.bigdata.study.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits) {
                    out.collect(Tuple2.of(split, 1));
                }
            }
        }).keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new SumProcessFunction())
                .print().setParallelism(1);

        env.execute("word window count");
    }

    public static class SumProcessFunction
            extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        public void process(String key,
                            Context context,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) {
            System.out.println("当前处理时间为：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间为：" + dateFormat.format(context.window().getStart()));
            System.out.println("窗口结束时间为：" + dateFormat.format(context.window().getEnd()));

            int count = 0;
            for(Tuple2<String, Integer> e : elements){
                count ++;
            }
            System.out.println("单词 " + key + "一共出现了 " + count + " 次");
            System.out.println("--------------------------------------------");
            out.collect(Tuple2.of(key, count));
        }
    }
}
