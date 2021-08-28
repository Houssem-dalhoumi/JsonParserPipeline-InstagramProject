package com.databeans

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("JsonParser")
      .master("local[*]")
      .getOrCreate()


    val commentsInfo = new ProfileCommentsInfo(spark)
    val postInfo = new PostInfo(spark)
    val profileInfo = new ProfileInfo(spark)

    val jsonData = spark.read.option("multiline", true).json("PhilCoutinho.json")

    val commentsTable = commentsInfo.buildCommentsDataFrame(jsonData)
    commentsTable.write.parquet("SilverProfileCommentsTable.parquet")
    commentsTable.createOrReplaceTempView("ProfileCommentsInfoTable")

    val postInfoTable = postInfo.buildPostInfoDataFrame(jsonData)
    postInfoTable.write.parquet("SilverPostInfoTable.parquet")
    postInfoTable.createOrReplaceTempView("PostInfoTable")

    val profileInfoTable = profileInfo.buildProfileInfoDataFrame(jsonData)
    profileInfoTable.write.parquet("SilverProfileInfoTable.parquet")
    profileInfoTable.createOrReplaceTempView("ProfileInfoTable")

  }

}
