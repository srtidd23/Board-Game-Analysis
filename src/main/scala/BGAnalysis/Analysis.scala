package BGAnalysis

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession, functions}

object Analysis extends java.io.Serializable {

  val spark = SparkSession.builder()
    .appName("Twitter Emoji Analysis")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


}
