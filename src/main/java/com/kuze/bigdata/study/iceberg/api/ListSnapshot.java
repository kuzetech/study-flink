package com.kuze.bigdata.study.iceberg.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import java.util.Map;

public class ListSnapshot {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.load("hdfs://localhost:8020/warehouse/iceberg_hadoop_db/iceberg_001");

        for (Snapshot snapshot : table.snapshots()) {
            System.out.println(snapshot.snapshotId());
        }

        Map<String, String> properties = table.properties();
        properties.forEach((key, value) ->{
            System.out.println(key + ":" + value);
        });

        Schema schema = table.schema();
        Map<Integer, PartitionSpec> specs = table.specs();

    }
}
