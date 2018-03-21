package template.spark

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


/**
  * create by liush on 2018-2-27
  */
object kafkaStructStreaming  extends InitSpark {

  def main(args: Array[String]) = {

    //val version = spark.version
    //println("SPARK VERSION = " + version)
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()
    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))

    val streamingDataFrame = spark.readStream.schema(mySchema).csv("/Users/apple/Idea/workspace/spark-gradle-template/data/moviedata.csv")

    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "topicName")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "path to your local dir")
      .start()

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topicName")
      .load()
    import org.apache.spark.sql.functions._
    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", mySchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")
    df1.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }
}
