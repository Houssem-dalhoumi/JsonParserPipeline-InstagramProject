package com.databeans

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

class PostInfo(spark : SparkSession) {

  import spark.implicits._

  def buildPostInfoDataFrame ( RawDataFrame : DataFrame ) : DataFrame = {
    val explodeDF = RawDataFrame.select(explode($"GraphImages"))
    val postInfo = explodeDF.select(
      $"col.comments_disabled" as 'is_comments_disabled ,
      'col.getItem("dimensions").getItem("height") as 'height,
      'col.getItem("dimensions").getItem("width") as 'width,
      'col.getItem("display_url")  as 'display_url,
      'col.getItem("edge_media_preview_like").getItem("count") as 'likes,
      'col.getItem("edge_media_to_comment").getItem("count") as 'count,
      'col.getItem("gating_info") as 'gating_info,
      'col.getItem("id") as 'Post_id,
      'col.getItem("is_video") as 'is_video,
      'col.getItem("location") as 'location,
      'col.getItem("media_preview") as 'media_preview,
      'col.getItem("owner").getItem("id") as 'username_id,
      'col.getItem("shortcode") as 'shortcode,
      'col.getItem("taken_at_timestamp").cast(TimestampType) as 'taken_at_timestamp,
      'col.getItem("thumbnail_src") as 'thumbnail_src,
      'col.getItem("username") as 'username
    )
    postInfo
  }
}
