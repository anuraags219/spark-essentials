package com.anuraag.hadoop.batch

import java.io.File

import com.anuraag.hadoop.App.ExecutionContext

object Model {

  val inputPath = "/home/anuraag/Downloads/stackexchange/christianity.meta.stackexchange.com/Badges.xml"
  val outputPath = "hdfs://localhost:9000/user/anuraag/hadoop_project/output/"
  val fileNames: Seq[String] = Seq("Badges", "Comments", "PostHistory", "PostLinks", "Posts", "Tags", "Users", "Votes")
  val rootPath = "/home/anuraag/Downloads/stackexchange/christianity.meta.stackexchange.com/"
  val extensions = List("xml")

  def execute(executionContext: ExecutionContext): Unit = {
//    val dataFrame = DataExploration.readXML(executionContext, inputPath, "row")
//    DataExploration.writeXML(executionContext, dataFrame, outputPath)
//    val dataFrames = DataExploration.readMultipleXML(executionContext, fileNames, rootPath)
//    dataFrames.foreach(dataFrame => dataFrame._2.show(5, false))
//    dataFrames.foreach(dataFrame => println(dataFrame._1))
//    dataFrames("Badges").show(5, false)
//    Regex.regexGeneral()
    val files = BulkTransformations.getListOfFiles(new File(rootPath), extensions)
    val dataFrames = BulkTransformations.readAllDataFrames(executionContext, files)
//    dataFrames.foreach(dataFrame => {
//      println(dataFrame._1)
//      dataFrame._2.show(1, false)
//      dataFrame._2.printSchema()
//    })
    //    files.foreach(file => println(file._1))

    val posts = DataFrameTransformations.getPostsData(executionContext, dataFrames)
//    posts.show()

    val badges = DataFrameTransformations.getBadges(executionContext, dataFrames)
    val joinedDF = badges.join(posts, Seq("_Id"), "left")
    joinedDF.show()

  }
}
