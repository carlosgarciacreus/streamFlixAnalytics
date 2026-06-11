package com.streamflix.Analisis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._

class Maraton {

  def extraccion(rdd: RDD[String]): RDD[(java.sql.Timestamp, Int, String, Int)] = {
    rdd.filter { linea =>
      val parts = linea.split("\\|")
      parts.length == 4 && parts(3).split(":")(1).forall(_.isDigit)
    }.map { linea =>
      val parts = linea.split("\\|")
      val formato = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val inicio = new java.sql.Timestamp(formato.parse(parts(0).split("]")(1).trim).getTime)
      val user = parts(1).split(":")(1).toInt
      val pelicula = parts(2).split(":")(1)
      val duracion = parts(3).split(":")(1).toInt
      (inicio, user, pelicula, duracion)
    }
  }


  def toDataFrame(spark: SparkSession, rdd: RDD[(java.sql.Timestamp, Int, String, Int)]): DataFrame = {
    import spark.implicits._
    rdd.toDF("Inicio", "usuario", "movie_id", "duracion")
  }

  def windowSpec(): WindowSpec = {
    Window.partitionBy("usuario").orderBy("Inicio")
  }

  def aplicarLag(df: DataFrame, ws: WindowSpec): DataFrame = {
    df.withColumn("prev_inicio", lag("Inicio", 1).over(ws))
  }

  def diferencia(df: DataFrame): DataFrame = {
    df.withColumn("Diferencia", (unix_timestamp(col("Inicio")) - unix_timestamp(col("prev_inicio"))) / 60)
  }

  def is_binge(df: DataFrame): DataFrame = {
    df.withColumn("is_binge", when(col("Diferencia") >= 0 && col("Diferencia") < 20, 1).otherwise(0))
  }

  def mostrar(df: DataFrame): DataFrame = {
    df.filter(col("is_binge") === 1).groupBy("usuario").count().orderBy(col("count").desc).limit(10)
  }

}
