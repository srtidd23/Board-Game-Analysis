package BGAnalysis

import BGAnalysis.Analysis.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions.udf

object DataFrameBuilder  extends java.io.Serializable {

  val spark = SparkSession.builder()
    .appName("Twitter Emoji Analysis")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")



  def topBGToDF(S3:Boolean = true, s3Folder: String = "inputs/"): DataFrame ={
    import spark.implicits._
    val idToMechanicUDF = udf((row: Row) => idToMechanic(row))
    spark.udf.register("idToMechanic",idToMechanicUDF)
    var df: DataFrame = spark.emptyDataFrame
    if(S3){
      // Replace Key with your AWS account key (You can find this on IAM  service)
      spark.sparkContext
        .hadoopConfiguration.set("fs.s3.access.key", spark.conf.get("AWS_ACCESS_KEY"))//System.getenv("AWS_ACCESS_KEY"))
      // Replace Key with your AWS secret key (You can find this on IAM  service)
      spark.sparkContext
        .hadoopConfiguration.set("fs.s3.secret.key", spark.conf.get("AWS_SECRET_KEY"))//System.getenv("AWS_SECRET_KEY"))
      spark.sparkContext
        .hadoopConfiguration.set("fs.s3.endpoint", "s3.amazonaws.com")
       df = spark.read.option("multiLine", true).json("s3://board-game-analysis/"+s3Folder).toDF()
    }
    else {
       df = spark.read.option("multiLine", true).json("top_500").toDF()
    }
    df.select(functions.explode($"games"))
      .select($"col.name", $"col.rank",$"col.min_players", $"col.max_players",
        $"col.min_playtime", $"col.max_playtime", $"col.price", $"col.year_published", $"col.average_user_rating",$"col.mechanics" )
      .withColumn("mechanics", functions.explode($"mechanics"))
      .withColumn("mechanics", idToMechanicUDF($"mechanics"))
  }

  def gameMechanicsToDF(): DataFrame ={
    import spark.implicits._
    val df = spark.read.option("multiline", true).json("game_mechanics").toDF()
    df.select(functions.explode($"mechanics"))
      .select($"col.id", $"col.name")
  }

  def idToMechanic(row: Row): String ={
    Mechanics_Dictionary.mechanics_Dictionary.get(row.get(0).toString).getOrElse("UNKNOWN")
  }

}
