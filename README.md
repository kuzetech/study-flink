# study-flink-iceberg-demo

## 项目主要内容

本项目主要使用 java + flink + iceberg + hadoop 测试 ETL功能

## 项目构建

项目主要运行在本地测试，在根目录的 docker-compose 文件夹中提供了 hdfs 环境安装脚本。

## 测试步骤

请优先参考 TableApiCommonOperate 这个类中创建环境和表的语句，才能进行后续测试。

## 特别记录

iceberg 其实就是实现了一个 flink 的自定义source，并且在 source 算子中通过状态记录了 lastSnapshotIdState。

另外 iceberg 还实现了自己的 sink。大致的原理分为以下几个步骤：

1. 先将数据写入 iceberg
2. 将提交状态（待提交到 iceberg 的 manifest 文件）存入到后端
3. 等到 checkpoint 完成后，提交 manifest 到 iceberg
4. 期间如果失败，程序重启后将重新提交 manifest 文件
