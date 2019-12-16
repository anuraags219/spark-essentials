package com.anuraag.hadoop.batch

import java.io.File

import com.anuraag.hadoop.App.ExecutionContext
import org.apache.spark.sql.DataFrame

object BulkTransformations {

  def getListOfFiles(dir: File, extensions: List[String]): Map[String, String] = {
    val files = dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
    val mappedFiles = files.map(file => {
      (file.getName.replace(".xml", ""), file.toString)
    }).toMap[String, String]
    mappedFiles
  }

  def readAllDataFrames(executionContext: ExecutionContext, fileList: Map[String, String]): Map[String, DataFrame] = {
    val dataFrameList = fileList.map(file => (file._1, DataExploration.readXML(executionContext, file._2, "row")))
    dataFrameList
  }

}
