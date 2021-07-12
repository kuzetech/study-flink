package com.kuze.bigdata.study.iceberg.manage_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.util.List;

public class ListTables {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://localhost:8020/warehouse";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        List<TableIdentifier> tables = catalog.listTables(Namespace.of("iceberg_hadoop_db"));
        for (TableIdentifier table : tables) {
            System.out.println(table.name());
        }

    }
}
