package com.kuze.bigdata.study.iceberg;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

public class DataStreamStreamingIncrementalRead {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader loader = TableLoader.fromHadoopTable("hdfs://localhost:8020/warehouse/iceberg_hadoop_db/iceberg_001");

        //默认十秒获取一次数据，第一次查询根据启动时间，非整十秒
        DataStream<RowData> batch = FlinkSource.forRowData()
                .env(env)
                .tableLoader(loader)
                .streaming(true)
                //仅在state中不存在时设置才有效，后续重启都从state中读取
                .startSnapshotId(755563362651967135L) //不包含当前
                .build();

        batch.map(new MapFunction<RowData, String>() {
            @Override
            public String map(RowData value) throws Exception {
                FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
                System.out.println(dateFormat.format(System.currentTimeMillis()));
                Long id = value.getLong(0);
                String data = value.getString(1).toString();
                return id + " : " + data;
            }
        }).print();

        env.execute("test");
    }
}
