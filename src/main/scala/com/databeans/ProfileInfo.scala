package com.databeans

import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProfileInfo(spark : SparkSession) {
  import spark.implicits._

  def buildProfileInfoDataFrame (rawDataFrame : DataFrame): DataFrame = {
    val profileInfo = rawDataFrame.select(
      $"GraphProfileInfo.created_time".cast(TimestampType) as 'created_time,
      $"GraphProfileInfo.username" as 'username,
      'GraphProfileInfo.getItem("info").getItem("biography") as 'biography,
      'GraphProfileInfo.getItem("info").getItem("followers_count") as 'followers_count,
      'GraphProfileInfo.getItem("info").getItem("following_count") as 'following_count,
      'GraphProfileInfo.getItem("info").getItem("full_name") as 'full_name,
      'GraphProfileInfo.getItem("info").getItem("id") as 'username_id,
      'GraphProfileInfo.getItem("info").getItem("is_business_account") as 'is_business_account,
      'GraphProfileInfo.getItem("info").getItem("is_joined_recently") as 'is_joined_recently,
      'GraphProfileInfo.getItem("info").getItem("is_private") as 'is_private,
      'GraphProfileInfo.getItem("info").getItem("posts_count") as 'posts_count,
      'GraphProfileInfo.getItem("info").getItem("profile_pic_url") as 'profile_pic_url
    )
    profileInfo
  }

}
