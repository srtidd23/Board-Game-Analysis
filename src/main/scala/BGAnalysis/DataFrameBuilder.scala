package BGAnalysis

import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.functions.udf

object DataFrameBuilder  extends java.io.Serializable {

  val spark = SparkSession.builder()
    .appName("Twitter Emoji Analysis")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")



  def top100ToDF(): Unit ={
    import spark.implicits._
    val idToMechanicUDF = udf((row: Row) => idToMechanic(row))
    spark.udf.register("idToMechanic",idToMechanicUDF)
    val df = spark.read.option("multiLine", true).json("top_100").toDF()
    df.select(functions.explode($"games"))
      .select($"col.name", $"col.rank",$"col.min_players", $"col.max_players",
        $"col.min_playtime", $"col.max_playtime", $"col.price", $"col.mechanics" )
      .withColumn("mechanics", functions.explode($"mechanics"))
      .withColumn("mechanics", idToMechanicUDF($"mechanics")).show()
  }

  def gameMechanicsToDF(): Unit ={
    import spark.implicits._
    val df = spark.read.option("multiline", true).json("game_mechanics").toDF()
    df.select(functions.explode($"mechanics"))
      .select($"col.id", $"col.name").show(200)
  }

  def idToMechanic(row: Row): String ={
    Mechanics_Dictionary.mechanics_Dictionary.get(row.get(0).toString).getOrElse("UNKNOWN")
  }

}
