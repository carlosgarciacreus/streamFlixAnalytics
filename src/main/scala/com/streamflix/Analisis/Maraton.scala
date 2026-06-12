package com.streamflix.Analisis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class Maraton {

  def extraccion(rdd: RDD[String]): RDD[(String, Int, String, Int)] = {
    rdd.filter { linea =>
      val parts = linea.split("\\|")
      parts.length == 4 && parts(3).split(":")(1).forall(_.isDigit)
    }.map { linea =>
      val parts = linea.split("\\|")
      val inicio = parts(0).split("]")(1).trim
      val user = parts(1).split(":")(1).toInt
      val pelicula = parts(2).split(":")(1).replace("Movie_", "")
      val duracion = parts(3).split(":")(1).toInt
      (inicio, user, pelicula, duracion)
    }
  }


  def toDataFrame(spark: SparkSession, rdd: RDD[(String, Int, String, Int)]): DataFrame = {
    import spark.implicits._
    rdd.toDF("Inicio", "usuario", "movie_id", "duracion")
      .withColumn("Inicio", to_timestamp(col("Inicio"),"yyyy-MM-dd HH:mm:ss"))
      .filter(col("Inicio").isNotNull)
  }

  def windowSpec(): WindowSpec = {
    Window.partitionBy("usuario").orderBy("Inicio")
  }

  def aplicarLag(df: DataFrame, ws: WindowSpec): DataFrame = {
    df.withColumn("prev_inicio", lag("Inicio", 1).over(ws))
  }

  def diferencia(df: DataFrame): DataFrame = {
    df.withColumn("Diferencia", (col("Inicio").cast(LongType) - col("prev_inicio").cast(LongType)) / 60)
  }

  def is_binge(df: DataFrame): DataFrame = {
    df.withColumn("is_binge", when(col("Diferencia") > 0 && col("Diferencia") < 20, 1).otherwise(0))
  }

  def mostrar(df: DataFrame): DataFrame = {
    df.filter(col("is_binge") === 1).groupBy("usuario").agg(count("*").alias("conteo")).orderBy(col("conteo").desc).limit(10)
  }

}
