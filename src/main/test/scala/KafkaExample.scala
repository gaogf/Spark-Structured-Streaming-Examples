// Databricks notebook source
// MAGIC %md ## Setup Connection to Kafka

// COMMAND ----------

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.ForeachWriter
import template.spark.InitSpark
object  kafkaExample extends InitSpark {

  def main(args: Array[String]) = {
    var streamingInputDF =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "<server:ip")
        .option("subscribe", "topic1")
        .option("startingOffsets", "latest")
        .option("minPartitions", "10")
        .option("failOnDataLoss", "true")
        .load()

    // COMMAND ----------
    streamingInputDF.printSchema
    // MAGIC %md ## streamingInputDF.printSchema
    // MAGIC timestamp是消息接受的时间戳
    // MAGIC   root <br><pre>
    // MAGIC    </t>|-- key: binary (nullable = true) <br>
    // MAGIC    </t>|-- value: binary (nullable = true) <br>
    // MAGIC    </t>|-- topic: string (nullable = true) <br>
    // MAGIC    </t>|-- partition: integer (nullable = true) <br>
    // MAGIC    </t>|-- offset: long (nullable = true) <br>
    // MAGIC    </t>|-- timestamp: timestamp (nullable = true) <br>
    // MAGIC    </t>|-- timestampType: integer (nullable = true) <br>

    // COMMAND ----------
    /**
      * JSON数据被推送到上述的Kafka <topic>
      *
      */
    val value = """{"city": "<CITY>",
  "country": "United States",
  "countryCode": "US",
  "isp": "<ISP>",
  "lat": 0.00,
  "lon": 0.00,
  "region": "CA",
  "regionName": "California",
  "status": "success",
  "hittime": "<TIMPSTAMP>",
  "zip": "<ZIP>"
}"""
    // MAGIC %md ## Sample Message
    // MAGIC <pre>
    // MAGIC {
    // MAGIC </t>"city": "<CITY>",
    // MAGIC </t>"country": "United States",
    // MAGIC </t>"countryCode": "US",
    // MAGIC </t>"isp": "<ISP>",
    // MAGIC </t>"lat": 0.00, "lon": 0.00,
    // MAGIC </t>"query": "<IP>",
    // MAGIC </t>"region": "CA",
    // MAGIC </t>"regionName": "California",
    // MAGIC </t>"status": "success",
    // MAGIC </t>"hittime": "2017-02-08T17:37:55-05:00",
    // MAGIC </t>"zip": "38917"
    // MAGIC }

    // COMMAND ----------

    // MAGIC %md ## GroupBy, Count

    // COMMAND ----------

    import org.apache.spark.sql.functions._
    import spark.implicits._
    //把邮编从JSON消息中提取出来,把他们group起来再计数,这些步骤全部是我们一边从kafka的topic读取数据一边实时处理的
    //比如多少用户是从某一个邮编地区来的,用户通过哪个ISP进入等等
    var streamingSelectDF =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"))
        .groupBy($"zip")
        .count()

    // COMMAND ----------

    // MAGIC %md ## Window

    // COMMAND ----------

    import org.apache.spark.sql.functions._
    //基于Window的处理
    //已经让parse,select,groupBy和count这些查询持续的在运行了
    //接下来如果我们想知道每个邮编的在10分钟内的总流量,并且从每个小时的第2分钟开始每5分钟跟新一次该怎么办呢?
    //进入的JSON数据包含一个表示时间戳的域'hittime',我们可以用这个域来查询每10分钟的总流量。
    //注意在结构化流处理中,基于window的处理被认为是一种groupBy操作
    var streamingSelectDF3 =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"), get_json_object(($"value").cast("string"), "$.hittime").alias("hittime"))
        //接下来如果我们想知道每个邮编的在10分钟内的总流量,并且从每个小时的第2分钟开始每5分钟跟新一次该怎么办呢?
        .groupBy($"zip", window($"hittime".cast("timestamp"), "10 minute", "5 minute", "2 minute"))
        .count()


    // COMMAND ----------

    // MAGIC %md ## Memory Output

    // COMMAND ----------

    import org.apache.spark.sql.streaming.ProcessingTime
    //输出选项
    //比如,如果我们需要debug,你可能选择控制台输出,如果我们希望数据一边被处理我们能一边实时查询数据,
    //内存输出则是合理的选择。类似的,输出也可以被写到文件,外部数据库,甚至可以重新流入Kafka。
    //数据被作为内存中的数据表存储起来。从内存中,用户可以对数据集用SQL进行查询,数据表的名字可以通过queryName选项来制定

    val query =
      streamingSelectDF3
        .writeStream
        //数据被作为内存中的数据表存储起来,从内存中,用户可以对数据集用SQL进行查询
        .format("memory")
        .queryName("isphits")
        .outputMode("complete")
        //同时数据会自动被更新
        .trigger(ProcessingTime("25 seconds"))
        .start()

    // COMMAND ----------

    // MAGIC %md ## Console Output

    // COMMAND ----------

    import org.apache.spark.sql.streaming.ProcessingTime

    val query6 =
      streamingSelectDF
        .writeStream
        //输出被直接打印到控台或者stdout日志
        .format("console")
        .outputMode("complete")
        //同时数据会自动被更新
        .trigger(ProcessingTime("25 seconds"))
        .start()

    // COMMAND ----------

    // MAGIC %md ## File Output with Partitions

    // COMMAND ----------

    import org.apache.spark.sql.functions._
    //动态地基于任何列对接受的消息进行分区
    //我们可以基于‘zipcode’或者‘day’进行分区,这可以让查询变得更快,因为通过引用一个个分区,一大部分数据都可以被跳过
    var streamingSelectDF2 =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.zip").alias("zip"),
          get_json_object(($"value").cast("string"), "$.hittime").alias("hittime"),
          date_format(get_json_object(($"value").cast("string"), "$.hittime"), "dd.MM.yyyy").alias("day"))
        .groupBy($"zip")
        .count()
        .as[(String, String)]

    // COMMAND ----------

    import org.apache.spark.sql.streaming.ProcessingTime

    val query2 =
      streamingSelectDF2
        .writeStream
        //文件输出长期存储的最佳方法,不像内存或者控台这样的接收系统,
        //文件和目录都是具有容错性的,所以,这个选项还要求一个“检查点”目录来存放一些为了容错性需要的状态.
        .format("parquet")
        .option("path", "/mnt/sample/test-data")
        .option("checkpointLocation", "/mnt/sample/check")
        //接下来我们可以把输入数据按照‘zip’和‘day’分区
        .partitionBy("zip", "day")
        .trigger(ProcessingTime("25 seconds"))
        .start()
    //数据被存储下来之后就可以像其他数据集一样在Spark中被查询了
    val streamData = spark.read.parquet("/mnt/sample/data")
    streamData.filter($"zip" === "38908").count()
    // COMMAND ----------

    // MAGIC %md ##### Create Table

    // COMMAND ----------
    /**
      * 分区的数据可以直接在数据集和DataFrames被使用,如果一个数据表创建的时候指向了这些文件被写入的文件夹,
      * Spark SQL可以用来查询这些数据。
      * %sql CREATE EXTERNAL TABLE  test_par
        (hittime string)
        PARTITIONED BY (zip string, day string)
        STORED AS PARQUET
        LOCATION '/mnt/sample/test-data'
      */

    //这种方法需要注意的一个细节是数据表需要被加入一个新的分区，数据表中的数据集才能被访问到
    /**
      * %sql ALTER TABLE test_par ADD IF NOT EXISTS
    PARTITION (zip='38907', day='08.02.2017') LOCATION '/mnt/sample/test-data/zip=38907/day=08.02.2017'
      */
    //分区引用也可以被预先填满,这样随时文件在其中被创建,他们可以立即被访问。
    //%sql select * from test_par
    // MAGIC %sql CREATE EXTERNAL TABLE  test_par
    // MAGIC     (hittime string)
    // MAGIC     PARTITIONED BY (zip string, day string)
    // MAGIC     STORED AS PARQUET
    // MAGIC     LOCATION '/mnt/sample/test-data'

    // COMMAND ----------

    // MAGIC %md ##### Add Partition

    // COMMAND ----------

    // MAGIC %sql ALTER TABLE test_par ADD IF NOT EXISTS
    // MAGIC     PARTITION (zip='38907', day='08.02.2017') LOCATION '/mnt/sample/test-data/zip=38907/day=08.02.2017'

    // COMMAND ----------

    // MAGIC %md ##### Select

    // COMMAND ----------

    // MAGIC %sql select * from test_par

    // COMMAND ----------

    // MAGIC %md ## JDBC Sink

    // COMMAND ----------

    import java.sql._
    //把流处理输出写到像MySQL这样的外部数据库中
    //我们可以用‘foreach’输出来写入数据库,让我们来写一个自定义的JDBCSink来继承ForeachWriter来实现集中的方法
    class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[(String, String)] {
      val driver = "com.mysql.jdbc.Driver"
      var connection:Connection = _
      var statement:Statement = _

      def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: (String, String)): Unit = {
        statement.executeUpdate("INSERT INTO zip_test " +
          "VALUES (" + value._1 + "," + value._2 + ")")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close
      }
    }


    // COMMAND ----------

    val url="jdbc:mysql://<mysqlserver>:3306/test"
    val user ="user"
    val pwd = "pwd"
    //JDBCSink
    //批处理完成后,每个邮编的总数就会被插入/更新到我们的MySQL数据库中了
    val writer3 = new JDBCSink(url,user, pwd)
    val query3 =
      streamingSelectDF2
        .writeStream
        .foreach(writer3)
        .outputMode("update")
        .trigger(ProcessingTime("25 seconds"))
        .start()

    // COMMAND ----------

    // MAGIC %md ## Kafka Sink

    // COMMAND ----------

    import java.util.Properties

    import org.apache.spark.sql.ForeachWriter

    //结构化流处理API还不原生支持"kafka"输出格式
    //到我们在将消息流入Kafka的topic2.我们每个批处理后会把更新后的zipcode:count传回Kafka
    class  KafkaSink(topic:String, servers:String) extends ForeachWriter[(String, String)] {
      val kafkaProperties = new Properties()
      kafkaProperties.put("bootstrap.servers", servers)
      kafkaProperties.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
      kafkaProperties.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
      val results = new scala.collection.mutable.HashMap[String, String]
      var producer: KafkaProducer[String, String] = _

      def open(partitionId: Long,version: Long): Boolean = {
        producer = new KafkaProducer(kafkaProperties)
        true
      }

      def process(value: (String, String)): Unit = {
        producer.send(new ProducerRecord(topic, value._1 + ":" + value._2))
      }

      def close(errorOrNull: Throwable): Unit = {
        producer.close()
      }
    }

    // COMMAND ----------

    val topic = "<topic2>"
    val brokers = "<server:ip>"
    //到我们在将消息流入Kafka的topic2.我们每个批处理后会把更新后的zipcode:count传回Kafka
    //随着消息被处理，在一次批处理中被更新的邮编会被送回Kafka，没被更新的邮编则不会被发送
    //你也可以“完全”模式运行，类似我们在上面数据库的例子里那样，这样所有的邮编的最近的计数都会被发送，即使有些邮编的总数与上次批处理比并没有变化。
    //
    val writer2 = new KafkaSink(topic, brokers)

    val query4 =
      streamingSelectDF2
        .writeStream
        .foreach(writer2)
        .outputMode("update")
        .trigger(ProcessingTime("25 seconds"))
        .start()
  }
}
