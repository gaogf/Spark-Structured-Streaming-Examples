package template.spark

/**
  * Created by ankur on 18.12.16.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object KafkaMain {

  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)

    logger.setLevel(Level.INFO)

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob()
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        logger.error(ex.getStackTrace.toString)
    }
  }
}

class SparkJob extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)
  logger.setLevel(Level.INFO)
  val sparkSession =
    SparkSession.builder
      .master("local[*]")
      .appName("kafka2Spark2Cassandra")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()


  def runJob() = {

    logger.info("Execution started with following configuration")
    val cols = List("user_id", "time", "event","afdadf")

    import sparkSession.implicits._
    val lines = sparkSession.readStream
      .format("kafka")
      .option("subscribe", "test.1")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)",
                  "CAST(topic as STRING)",
                  "CAST(partition as INTEGER)")
      .as[(String, String, Integer)]

    val schema=Array("year","month","day")
    //数据字段的拆分
    val pdp=lines.rdd.map(arr=>{
      val p= arr._2.split("-")
      Row.fromSeq(p)
    })
    val schemaa=StructType(List(StructField("year",StringType,true),StructField("month",StringType,true),StructField("day",StringType,true)))
    val tr=sparkSession.createDataFrame(pdp,schemaa)
    tr.show()

  val linesMap=lines.map { line =>
    //value以逗号分隔发送的值"userid_1;2015-05-01T00:00:00;some_value"
    //取出第一列的值(value)
    val columns = line._1.split(";") // value being sent out as a comma separated value "userid_1;2015-05-01T00:00:00;some_value"
    columns
  }.toDF(cols: _*)

    val df =
      lines.map { line =>
        //value以逗号分隔发送的值"userid_1;2015-05-01T00:00:00;some_value"
        //取出第一列的值(value)
        val columns = line._1.split(";") // value being sent out as a comma separated value "userid_1;2015-05-01T00:00:00;some_value"
        //(columns(0), columns(1), columns(2))
        columns
      }.toDF(cols: _*)

    df.printSchema()

    // Run your business logic here
    //在这里运行业务逻辑
   // val ds = df.select($"user_id", $"time", $"event").as[Commons.UserEvent]

   val query =
     df.writeStream.queryName("kafka2Spark2Cassandra").start

    query.awaitTermination()
    sparkSession.stop()
  }
}
