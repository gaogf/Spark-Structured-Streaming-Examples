package cassandra.StreamSinkProvider

import cassandra.{CassandraDriver, CassandraKafkaMetadata}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions.max
import spark.SparkHelper
import cassandra.CassandraDriver
import com.datastax.spark.connector._
import kafka.KafkaMetadata
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types.LongType
import radio.SimpleSongAggregation

/**
* must be idempotent and synchronous (@TODO check asynchronous/synchronous from Datastax's Spark connector) sink
  * 必须是幂等和同步的(@TODO检查来自Datastax的Spark连接器的异步/同步)接收器
*/
class CassandraSink() extends Sink {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._
  import org.apache.spark.sql.functions._

  private def saveToCassandra(df: DataFrame) = {
    val ds = CassandraDriver.getDatasetForCassandra(df)
    ds.show() //Debug only

    ds.rdd.saveToCassandra(CassandraDriver.namespace,
      CassandraDriver.StreamProviderTableSink,
      SomeColumns("title", "artist", "radio", "count")
    )
    //保存元数据
    saveKafkaMetaData(df)
  }

  /*
   * As per SPARK-16020 arbitrary transformations are not supported, but
   * converting to an RDD allows us to do magic.
   * 根据SPARK-16020,不支持任意转换,但转换为RDD可以让我们实现魔法
   */
  override def addBatch(batchId: Long, df: DataFrame) = {
    println(s"CassandraSink - Datastax's saveToCassandra method -  batchId : ${batchId}")
    saveToCassandra(df)
  }

  /**
    * saving the highest value of offset per partition when checkpointing is not available (application upgrade for example)
    * 当检查点不可用时保存每个分区的最高偏移值(例如应用程序升级)
    * http://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlTransactionsDiffer.html
    * should be done in the same transaction as the data linked to the offsets
    *应该与链接到偏移的数据在同一个事务中完成
    */
  private def saveKafkaMetaData(df: DataFrame) = {
    val kafkaMetadata = df
      .groupBy($"partition")
      .agg(max($"offset").cast(LongType).as("offset"))
      .as[KafkaMetadata]
    //保存Kafka元数据(每个主题的分区和偏移量(在我们的例子中只有一个))
    println("Saving Kafka Metadata (partition and offset per topic (only one in our example)")
    kafkaMetadata.show()

    kafkaMetadata.rdd.saveToCassandra(CassandraDriver.namespace,
      CassandraDriver.kafkaMetadata,
      SomeColumns("partition", "offset")
    )

    //Otherway to save offset inside Cassandra
    //其他方式来保存Cassandra内部的偏移量
    //kafkaMetadata.collect().foreach(CassandraKafkaMetadata.save)
  }
}

