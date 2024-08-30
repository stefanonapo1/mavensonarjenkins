package com.example

import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Simple Scala Spark Example")
      .master("local[*]") // Run locally with all cores
      .getOrCreate()

    // Simple test: Create a DataFrame from a collection
    val data = Seq("Hello", "World", "from", "Spark")
    val df = spark.createDataFrame(data.map(Tuple1(_))).toDF("words")

    // Show the DataFrame content
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}
