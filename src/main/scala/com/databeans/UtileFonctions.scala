package com.databeans

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, SparkSession}

class UtileFonctions(spark : SparkSession) {

  import spark.implicits._

  def getProfileId (idColumnName : String) : Column = {
    $"GraphProfileInfo.info.id" as idColumnName

  }

  def getPostId (columnExploded : String , idColumnName : String): Column = {
    col(columnExploded).getItem("id") as idColumnName
  }

}
