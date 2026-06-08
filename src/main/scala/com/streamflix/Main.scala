package com.streamflix

import org.apache.spark.sql.SparkSession

object Main extends App {

  // Inicializar SparkSession
  val spark = SparkSession.builder()
    .appName("StreamFlixAnalytics")
    .master("local[*]")
    .getOrCreate()

  // Obtener SparkContext
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val rawLogsRDD = sc.textFile("src/main/resources/data/server_logs.txt")

  spark.stop()
}
