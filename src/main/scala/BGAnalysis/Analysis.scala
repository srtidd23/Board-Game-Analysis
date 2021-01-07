package BGAnalysis

import org.apache.commons.math3.util.FastMath.sqrt
import org.apache.spark.sql.functions.{asc, avg, collect_list, count, desc, udf}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions}

import scala.collection.mutable.ArrayBuffer

object Analysis extends java.io.Serializable {

  val spark = SparkSession.builder()
    .appName("Twitter Emoji Analysis")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 1000)


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
   * Gives the correlation between a board game's average user rating and price as well as the correlation
   * between average user rating and average playtime
   * @param top100 the top 100 board games DataFrame
   */
  def question2(top100: DataFrame): Unit={
    import spark.implicits._

    val distinct100DF = top100.select($"average_user_rating", $"price", ($"min_playtime"+$"max_playtime")/2 as "average_playtime" ).distinct()

    val rankArray = distinct100DF.select($"average_user_rating").collect().map( _.get(0).toString.toDouble)
    val priceArray = distinct100DF.select($"price").collect().map(_.get(0).toString.toDouble)
    val playtimeArray = distinct100DF.select($"average_playtime").collect().map(_.get(0).toString.toDouble)

    val rankArrayNorm = normalize(rankArray)
    val priceArrayNorm = normalize(priceArray)
    val playtimeArrayNorm = normalize(playtimeArray)

    val priceCorr = correlation(rankArrayNorm, priceArrayNorm)
    val playtimeCorr = correlation(rankArrayNorm, playtimeArrayNorm)

    println(s"Rating and Price correlation: ${priceCorr}")
    println(s"Rating and Playtime correlation: ${playtimeCorr}")
  }


  /**
   * Shows the popularity of multiplayer vs solo modes in board games
   * @param top100 the top 100 board games DataFrame
   */
  def question3(top100: DataFrame): Unit ={
    import spark.implicits._
    val multiDF = top100.select($"name", $"rank", $"min_players", $"max_players").distinct().filter($"min_players" > 1 )
    val soloDF = top100.select($"name", $"rank", $"min_players", $"max_players").distinct().filter($"min_players" === 1)

    val multiCount = multiDF.count()
    val soloCount = soloDF.count()

    println(s"${multiCount} Multiplayer board games to ${soloCount} Solo/Offer Solo board games")
  }

  /**
   * Shows the top mechanics from board games in terms of popularity
   * @param top100 the top 100 board games DataFrame
   */
  def question4A(top100: DataFrame, outputPath: String = "output/question4A"): Unit ={
    import spark.implicits._
    val mechanicsDF = top100.groupBy($"mechanics").agg(count($"mechanics") as "count")
      .orderBy(desc("count"))
    mechanicsDF.write.mode(SaveMode.Overwrite).format("csv").save(outputPath)
    mechanicsDF.show()
  }

  /**
   * Shows the top mechanics that lead to better average board game ratings
   * @param top100 the top 100 board games DataFrame
   */
  def question4B(top100: DataFrame, outputPath: String = "output/question4B"): Unit ={
    import spark.implicits._

    top100.groupBy($"mechanics").agg(count($"mechanics") as "count")
      .orderBy(desc("count")).createOrReplaceTempView("mechanicsCount")
    top100.groupBy($"mechanics").agg(avg($"average_user_rating") as "average_rating")
      .orderBy(asc("average_rating")).filter($"mechanics" !== "UNKNOWN").createOrReplaceTempView("mechanicsScore")
    val result = spark.sql("SELECT mechanicsScore.mechanics, mechanicsCount.count, mechanicsScore.average_rating" +
      " FROM mechanicsScore JOIN mechanicsCount " +
      "ON mechanicsScore.mechanics == mechanicsCount.mechanics " +
      "WHERE mechanicsCount.count >= 3 " +
      "ORDER BY mechanicsScore.average_rating DESC;")
    result.write.mode(SaveMode.Overwrite).format("csv").save(outputPath)
    result.show()
  }


  /**
   * Shows the top publishing years with the highest average rated board games
   * @param top100 the top 100 board games DataFrame
   */
  def question5(top100: DataFrame, outputPath: String = "output/question5"): Unit ={
    import spark.implicits._
    val yearCountDF = top100.select($"year_published", $"name").distinct().groupBy($"year_published").count().createOrReplaceTempView("yearCount")
    val yearRankDF = top100.groupBy($"year_published").agg(avg($"average_user_rating") as "average_rating").orderBy(asc("average_rating")).createOrReplaceTempView("yearRank")
    val result = spark.sql("SELECT yearRank.year_published, yearCount.count, yearRank.average_rating " +
      "FROM yearRank JOIN yearCount " +
      "ON yearRank.year_published == yearCount.year_published " +
      "WHERE yearCount.count >= 3 " +
      "ORDER BY yearRank.average_rating DESC;")
    result.write.mode(SaveMode.Overwrite).format("csv").save(outputPath)
    result.show()
  }

}
