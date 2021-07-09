package com.kuze.bigdata.study.iceberg;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.iceberg.flink.FlinkTableOptions;

public class TableApiBatchInsertInto {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tenv = TableEnvironment.create(settings);
        tenv.getConfig().getConfiguration().set(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);

        // 使用table api 创建 hadoop catalog
        TableResult tableResult = tenv.executeSql(
                "CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://localhost:8020/warehouse',\n" +
                "  'property-version'='1'\n" +
                ")");

        // 使用catalog
        tenv.useCatalog("hadoop_catalog");
        // 创建库
        //tenv.executeSql("CREATE DATABASE if not exists iceberg_hadoop_db");
        tenv.useDatabase("iceberg_hadoop_db");

        // InsertInto
        tenv.executeSql("insert into hadoop_catalog.iceberg_hadoop_db.iceberg_002 select id,data from hadoop_catalog.iceberg_hadoop_db.iceberg_001");

    }
}
