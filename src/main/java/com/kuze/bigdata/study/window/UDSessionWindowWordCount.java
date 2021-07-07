package com.kuze.bigdata.study.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UDSessionWindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split(",");
                        for (String word : split) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .process(new SessionProcessFunction())
                .print();

        env.execute("UDSession window word count");
    }

    private static class CountWithTimeStamp {
        public String key;
        public int count;
        public long lastUpdateTime;
    }

    public static class SessionProcessFunction
            extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<CountWithTimeStamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(
                    new ValueStateDescriptor<CountWithTimeStamp>("my-state", CountWithTimeStamp.class));
        }

        @Override
        public void processElement(
                Tuple2<String, Integer> value,
                Context ctx,
                Collector<Tuple2<String, Integer>> out) throws Exception {

            long currentTime = System.currentTimeMillis();
            CountWithTimeStamp countWithTimeStamp = state.value();

            if(countWithTimeStamp == null){
                countWithTimeStamp = new CountWithTimeStamp();
                countWithTimeStamp.key = value.f0;
                countWithTimeStamp.count = 1;
                countWithTimeStamp.lastUpdateTime = currentTime;
            }else{
                countWithTimeStamp.count = countWithTimeStamp.count + value.f1;
                countWithTimeStamp.lastUpdateTime = currentTime;
            }

            state.update(countWithTimeStamp);

            ctx.timerService().registerProcessingTimeTimer(currentTime + 5000);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            CountWithTimeStamp countWithTimeStamp = state.value();
            if(timestamp >= (countWithTimeStamp.lastUpdateTime + 5000)){
                out.collect(Tuple2.of(countWithTimeStamp.key, countWithTimeStamp.count));
                state.clear();
            }
        }
    }


}
