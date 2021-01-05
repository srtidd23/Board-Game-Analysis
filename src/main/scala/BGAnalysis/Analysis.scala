package BGAnalysis

import org.apache.commons.math3.util.FastMath.sqrt
import org.apache.spark.sql.functions.{collect_list, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.mutable.ArrayBuffer

object Analysis extends java.io.Serializable {

  val spark = SparkSession.builder()
    .appName("Twitter Emoji Analysis")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


  /**
   * The correlation between two series of data ~1 = positive correlation,
   * ~0 = no correlation, ~-1 = -negative correlation
   * @param xArray the independent data series
   * @param yArray the dependent data series
   * @return the correlation number as a double
   */
  def correlation(xArray: Array[Double], yArray: Array[Double]):Double={
    var r = 0.0
    var x = 0.0
    var y = 0.0
    var x_2 = 0.0
    var y_2 = 0.0
    var xy = 0.0
    val n = xArray.length
    for(i <- (0 to (xArray.length - 1))){
      x += xArray(i)
      y += yArray(i)
      x_2 += (xArray(i)*xArray(i))
      y_2 += (yArray(i)*yArray(i))
      xy += (xArray(i)*yArray(i))
    }
    r = (n*xy - (x*y))/(sqrt(n*x_2 - (x*x)) * sqrt(n*y_2 - (y*y)))
    r
  }


  def normalize(array: Array[Double]): Array[Double]={
    val min = array.min
    val max = array.max

    array.map(value => (value - min)/(max - min))
  }

  /**
   * Shows the top 10 board games
   * @param top100 data frame containing the top 100 board games
   */
  def question1(top100: DataFrame): Unit={
    import spark.implicits._
    top100.groupBy($"name", $"rank", $"min_players", $"max_players",
      $"min_playtime", $"max_playtime", $"price").agg(collect_list($"mechanics").as("mechanics")).orderBy($"rank")
      .show(10)
  }

  /**
   * Gives the correlation between a board game's rank and price as well as the correlation
   * between rank and average playtime
   * @param top100 the top 100 board games DataFrame
   */
  def question2(top100: DataFrame): Unit={
    import spark.implicits._

    val distinct100DF = top100.select($"rank", $"price", ($"min_playtime"+$"max_playtime")/2 as "average_playtime" ).distinct()

    val rankArray = distinct100DF.select($"rank").collect().map(100.0 - _.get(0).toString.toDouble)
    val priceArray = distinct100DF.select($"price").collect().map(_.get(0).toString.toDouble)
    val playtimeArray = distinct100DF.select($"average_playtime").collect().map(_.get(0).toString.toDouble)

    val rankArrayNorm = normalize(rankArray)
    val priceArrayNorm = normalize(priceArray)
    val playtimeArrayNorm = normalize(playtimeArray)

    val priceCorr = correlation(rankArrayNorm, priceArrayNorm)
    val playtimeCorr = correlation(rankArrayNorm, playtimeArrayNorm)

    println(s"Rank and Price correlation: ${priceCorr}")
    println(s"Rank and Playtime correlation: ${playtimeCorr}")
  }

}
