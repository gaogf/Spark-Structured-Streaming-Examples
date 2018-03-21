package template.spark


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class monDay(yea:String,month:String,day:String)
object MainColumnSplit extends InitSpark {
  def main(args: Array[String]) = {

    val version = spark.version
    println("SPARK VERSION = " + version)

    import spark.implicits._
    val data=reader.csv("/Users/apple/Idea/workspace/spark-gradle-template/data/p20170501.csv")
    //data.
   // data.show()
    val schema=Array("year","month","day")
    //第一种转化方式
    val pdp=data.rdd.map(arr=>{
     val p= arr.getString(1).split("-")
      Row.fromSeq(p)
    })
    val schemaa=StructType(List(StructField("year",StringType,true),StructField("month",StringType,true),StructField("day",StringType,true)))
    //创建DataFrame
    val tr=spark.createDataFrame(pdp,schemaa)
    tr.show()
    val cols = List("user_id", "time", "event","afdadf")
    //第二种转发方式case class
    val linesMap=data.map { line =>
      //value以逗号分隔发送的值"userid_1;2015-05-01T00:00:00;some_value"
      //取出第一列的值(value)
      val columns = line.getString(1).split("-") // value being sent out as a comma separated value "userid_1;2015-05-01T00:00:00;some_value"
      monDay(columns(0),columns(1),columns(2))
    }
    linesMap.show
    //pd.show
    /* data.limit(10).foreach(row=>{
      println(row.getString(1).split("-").mkString(","))
    })*/

    close
  }
}
