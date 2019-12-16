package com.anuraag.hadoop.batch

import com.anuraag.hadoop.App.ExecutionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, sum, to_date}

object DataFrameTransformations {

  def dateConversion(executionContext: ExecutionContext, dataFrame: DataFrame): Unit = {
    dataFrame.show(1, false)
    dataFrame.printSchema()
    val newDF = dataFrame.withColumn("_Date", to_date(col("_Date")))
    newDF.show(1, false)
    newDF.printSchema()
  }

  def getPostsData(executionContext: ExecutionContext, dataFrames: Map[String, DataFrame]): DataFrame = {
    val posts = dataFrames("Posts").select("_Id", "_PostTypeId", "_ParentId", "_ViewCount", "_Score", "_Title", "_OwnerDisplayName",
                          "_AnswerCount", "_Body", "_ClosedDate")
    posts.select("*")
      .where(col("_PostTypeId") === lit(1))
      .groupBy(col("_Id"), col("_PostTypeId")).agg(sum(col("_ViewCount")).as("_ViewCountSum"))
      .orderBy(col("_ViewCountSum").desc)
  }

  def getBadges(executionContext: ExecutionContext, dataFrames: Map[String, DataFrame]): DataFrame = {
    import executionContext.spark.implicits._
    val badges = dataFrames("Badges").select($"_Id", $"_Name", $"_UserId", to_date(col("_Date")).as("_Date"))
        .drop()
    badges
  }

}
