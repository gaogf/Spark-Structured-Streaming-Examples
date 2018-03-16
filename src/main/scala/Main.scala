package main

import cassandra.CassandraDriver
import elastic.ElasticSink
import kafka.{KafkaSink, KafkaSource}
import mapGroupsWithState.MapGroupsWithState
import parquetHelper.ParquetService
import spark.SparkHelper

object Main {

  def main(args: Array[String]) {
    val spark = SparkHelper.getAndConfigureSparkSession()

    //Classic Batch
    //ParquetService.batchWay()

    //Streaming way
    //Generate a "fake" stream from a parquet file
    //从parquet文件生成“假”流
    val streamDS = ParquetService.streamingWay()

    val songEvent = ParquetService.streamEachEvent

    ElasticSink.writeStream(songEvent)

    //Send it to Kafka for our example
    //以我们为例,将它发送给Kafka
    KafkaSink.writeStream(streamDS)

    //Finally read it from kafka, in case checkpointing is not available we read last offsets saved from Cassandra
    //最后从kafka读取它,如果检查点不可用,我们读取从Cassandra保存的最后偏移量
    val (startingOption, partitionsAndOffsets) = CassandraDriver.getKafaMetadata()
    val kafkaInputDS = KafkaSource.read(startingOption, partitionsAndOffsets)

    //Just debugging Kafka source into our console
    //只需将Kafka源码调试到我们的控制台即可
    KafkaSink.debugStream(kafkaInputDS)

    //Saving using Datastax connector's saveToCassandra method
    //使用Datastax连接器的saveToCassandra方法保存
    CassandraDriver.saveStreamSinkProvider(kafkaInputDS)

    //Saving using the foreach method
    //使用foreach方法保存
    //CassandraDriver.saveForeach(kafkaInputDS) //Untype/unsafe method using CQL  --> just here for example

    //Another fun example managing an arbitrary state
    //管理任意状态的另一个有趣的例子
    MapGroupsWithState.write(kafkaInputDS)

    //Wait for all streams to finish
    //等待所有流完成
    spark.streams.awaitAnyTermination()
  }
}
