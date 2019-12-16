package com.anuraag.hadoop.batch

import java.io.File

import com.anuraag.hadoop.App.ExecutionContext

object Model {

  val inputPath = "/home/anuraag/Downloads/stackexchange/christianity.meta.stackexchange.com/Badges.xml"
  val outputPath = "hdfs://localhost:9000/user/anuraag/hadoop_project/output/"
  val fileNames: Seq[String] = Seq("Badges", "Comments", "PostHistory", "PostLinks", "Posts", "Tags", "Users", "Votes")
  val metaRootPath = "/home/anuraag/Downloads/stackexchange/christianity.meta.stackexchange.com/"
  val completeRootPath = "/home/anuraag/Downloads/stackexchange/christianity.stackexchange.com/"
  val extensions = List("xml")

  def execute(executionContext: ExecutionContext): Unit = {
//    val dataFrame = DataExploration.readXML(executionContext, inputPath, "row")
//    DataExploration.writeXML(executionContext, dataFrame, outputPath)
//    val dataFrames = DataExploration.readMultipleXML(executionContext, fileNames, rootPath)
//    dataFrames.foreach(dataFrame => dataFrame._2.show(5, false))
//    dataFrames.foreach(dataFrame => println(dataFrame._1))
//    dataFrames("Badges").show(5, false)
//    Regex.regexGeneral()
    val metaFiles = BulkTransformations.getListOfFiles(new File(metaRootPath), extensions)
    val metaDataFrames = BulkTransformations.readAllDataFrames(executionContext, metaFiles)
    println("Meta")
    metaDataFrames.foreach(dataFrame => {
      println(dataFrame._1)
      println(dataFrame._2.count())
//      dataFrame._2.printSchema()
    })

    val completeFiles = BulkTransformations.getListOfFiles(new File(completeRootPath), extensions)
    val completeDataFrames = BulkTransformations.readAllDataFrames(executionContext, completeFiles)
    println("Complete")
    completeDataFrames.foreach(dataFrame => {
      println(dataFrame._1)
      println(dataFrame._2.count())
      //      dataFrame._2.printSchema()
    })
    //    files.foreach(file => println(file._1))

//    val posts = DataFrameTransformations.getPostsData(executionContext, metaDataFrames)
//    posts.show()

//    val badges = DataFrameTransformations.getBadges(executionContext, metaDataFrames)
//    val joinedDF = badges.join(posts, Seq("_Id"), "left")
//    joinedDF.show()

  }
}
