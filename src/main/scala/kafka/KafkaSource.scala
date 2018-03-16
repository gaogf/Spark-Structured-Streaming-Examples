package kafka

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.types.{StringType, _}
import radio.{SimpleSongAggregation, SimpleSongAggregationKafka}
import spark.SparkHelper

/**
 @see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 */
object KafkaSource {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  /**
    * will return, we keep some kafka metadata for our example, otherwise we would only focus on "radioCount" structure
    * 将返回,为我们的例子保留一些kafka元数据,否则我们只会关注“radioCount”结构
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true) : KEPT
     |-- partition: integer (nullable = true) : KEPT
     |-- offset: long (nullable = true) : KEPT
     |-- timestamp: timestamp (nullable = true) : KEPT
     |-- timestampType: integer (nullable = true)
     |-- radioCount: struct (nullable = true)
     |    |-- title: string (nullable = true)
     |    |-- artist: string (nullable = true)
     |    |-- radio: string (nullable = true)
     |    |-- count: long (nullable = true)

    * @return
    *
    *
    * startingOffsets should use a JSON coming from the lastest offsets saved in our DB (Cassandra here)
    * startsOffsets应该使用来自我们数据库中保存的最新偏移量的JSON(这里是Cassandra)
    */
    def read(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest") : Dataset[SimpleSongAggregationKafka] = {
      println("Reading from Kafka")

      spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KafkaService.topicName)
      .option("enable.auto.commit", false) // Cannot be set to true in Spark Strucutured Streaming https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
      .option("group.id", "Structured-Streaming-Examples")
        //当开始一个新的kafka(默认位置是临时的(/tmp)而cassandra不是(var/lib)时,我们在Cassandra中保存了不同的偏移量,而不是kafka中的实际偏移量(不包含任何内容)
      .option("failOnDataLoss", false) // when starting a fresh kafka (default location is temporary (/tmp) and cassandra is not (var/lib)), we have saved different offsets in Cassandra than real offsets in kafka (that contains nothing)
        //这仅适用于新查询已启动并且恢复始终会从查询停止的位置启动
      .option(startingOption, partitionsAndOffsets) //this only applies when a new query is started and that resuming will always pick up from where the query left off
      .load()
      .withColumn(KafkaService.radioStructureName, // nested structure with our json,与我们的json嵌套结构
        from_json($"value".cast(StringType), KafkaService.schemaOutput) //From binary to JSON object
      ).as[SimpleSongAggregationKafka]
        //TODO找到更好的方法来过滤不良的JSON
      .filter(_.radioCount != null) //TODO find a better way to filter bad json
  }
}
