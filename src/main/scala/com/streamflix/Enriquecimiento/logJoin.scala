package com.streamflix.Enriquecimiento
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

class logJoin {

  def extraccion(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd.filter { linea =>
      val parts = linea.split("\\|")
      parts.length == 4 && parts(3).split(":")(1).forall(_.isDigit)
    }.map { linea =>
      val parts = linea.split("\\|")
      val movie = parts(2).split(":")(1).replace("Movie_", "")
      val duration = parts(3).split(":")(1).toInt
      (movie, duration)
    }
  }

  def toDataFrame(spark: SparkSession, rdd: RDD[(String, Int)]): DataFrame = {
    import spark.implicits._
    rdd.toDF("movie_id", "duration_watched")
  }

  def broadcastJoin(logsDF: DataFrame, moviesDF: DataFrame): DataFrame = {
    val enrichedDF = logsDF.join(
      broadcast(moviesDF),
      logsDF("movie_id") === moviesDF("id"),
      "inner"
    )
    enrichedDF
  }

}
