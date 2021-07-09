package com.kuze.bigdata.study.iceberg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

public class DataStreamBatchIncrementalRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader loader = TableLoader.fromHadoopTable("hdfs://localhost:8020/warehouse/iceberg_hadoop_db/iceberg_001");

        // 1168569634282321771L
        // 725714797831776415L
        // 755563362651967135L

        DataStream<RowData> batch = FlinkSource.forRowData()
                .env(env)
                .tableLoader(loader)
                .streaming(false)
                .startSnapshotId(1168569634282321771L) //不包含当前
                .endSnapshotId(755563362651967135L) //包含当前
                .build();

        batch.map(new MapFunction<RowData, String>() {
            @Override
            public String map(RowData value) throws Exception {
                Integer id = value.getInt(0);
                String data = value.getString(1).toString();
                return id + " : " + data;
            }
        }).print();

        env.execute("test");
    }
}
