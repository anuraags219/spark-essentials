package com.anuraag.hadoop.batch

import com.anuraag.hadoop.App.ExecutionContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.databricks.spark.xml._

object DataExploration {

  def readXML(executionContext: ExecutionContext, inputPath: String, rowTag: String): DataFrame = {
    executionContext.spark.read
      .option("rowTag", rowTag)
      .xml(inputPath)
  }

  def writeXML(executionContext: ExecutionContext, dataFrame: DataFrame, outputPath: String): Unit = {
    dataFrame.coalesce(1).write.mode(SaveMode.Overwrite).csv(outputPath)
  }

  def readMultipleXML(executionContext: ExecutionContext, fileNames: Seq[String], rootPath: String): Map[String, DataFrame] = {

    val dataFrames = fileNames.map(file => {
      val path = s"$rootPath$file.xml"
      val dataFrame = executionContext.spark.read
        .option("rowTag", "row")
        .xml(path)
      (file, dataFrame)
    }).toMap[String, DataFrame]
    dataFrames

  }

}
