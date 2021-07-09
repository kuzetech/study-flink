package com.kuze.bigdata.study.iceberg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.Random;

public class DataStreamWrite {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 只能使用batch写入的方式
        //DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        DataStreamSource<String> source = env.fromElements("aaaa", "bbb", "ccc");

        SingleOutputStreamOperator<RowData> input = source.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String value){
                System.out.println(value);
                GenericRowData rowData = new GenericRowData(2);
                rowData.setField(0, new Random().nextLong());

                rowData.setField(1, StringData.fromString(value));
                return rowData;
            }
        });

        Configuration hadoopConf = new Configuration();

        TableLoader tableLoader = TableLoader.fromHadoopTable(
                "hdfs://localhost:8020/warehouse/iceberg_hadoop_db/iceberg_001",
                hadoopConf);

        FlinkSink.forRowData(input)
                .tableLoader(tableLoader)
                .overwrite(false)
                .build();

        env.execute("Test Iceberg DataStream");
    }
}
