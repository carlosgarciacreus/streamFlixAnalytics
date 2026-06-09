package com.streamflix.Estandarizacion

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class MovieCleaner {

  def limpiarPrecio(df: DataFrame): DataFrame = {
    df.withColumn("subscription_price",
      regexp_replace(col("subscription_price"), "\\$", "").cast("Double"))
  }

  def nulosDuplicados(df: DataFrame) = {
    val nulos = df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*)
    nulos.show()

    val duplicados = df.count() - df.dropDuplicates().count()
    println(s"Duplicados: $duplicados")

  }

  def genresNulos(df: DataFrame) = {
    df.withColumn("genres",
      when(col("genres").isNull || col("genres") === "", "Unknown")
        .otherwise(col("genres")))
  }

}
