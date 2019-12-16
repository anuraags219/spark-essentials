package com.anuraag.hadoop

import com.anuraag.hadoop.batch.Model
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
      start(args)
  }

  case class ExecutionContext(spark: SparkSession)

  def start(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Hadoop Essentials").master("local[*]").getOrCreate()
    val executionContext = ExecutionContext(spark)
    executionContext.spark.sparkContext.setLogLevel("Error")
    Model.execute(executionContext)
  }
}