package com.databeans

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProfileCommentsInfo(spark: SparkSession) {
  import spark.implicits._

  val utileFonctions = new UtileFonctions(spark)
def getCommentsData (rawDataframe : DataFrame): DataFrame = {
  val explodedjson = rawDataframe.select( utileFonctions.getProfileId("Profile_id"),explode($"GraphImages").alias("GraphImages"))
   val commentsData = explodedjson.select(
     $"Profile_id",
     utileFonctions.getPostId("GraphImages": String, "Post_id": String)
     ,explode($"GraphImages.comments.data")
   )
  commentsData
}

  def buildCommentsDataFrame (rawDataFrame : DataFrame): DataFrame = {
    getCommentsData(rawDataFrame).select(
      $"Profile_id",
      $"Post_id",
      'col.getItem("created_at").cast(TimestampType) as 'created_at,
      'col.getItem("id") as'data_id,
      'col.getItem("text") as 'text,
      'col.getItem("owner").getItem("profile_pic_url") as 'profile_pic_url,
      'col.getItem("owner").getItem("id") as 'owner_id,
      'col.getItem("owner").getItem("username") as 'usernameOfCommentator
    )
  }
}
