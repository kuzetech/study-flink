package com.kuze.bigdata.study.iceberg;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader loader = TableLoader.fromHadoopTable("file://Users/huangsw/temp");

        DataStream<RowData> batch = FlinkSource.forRowData()
                .env(env)
                .tableLoader(loader)
                .streaming(false)
                .build();

        batch.print();

        env.execute("test");
    }
}
