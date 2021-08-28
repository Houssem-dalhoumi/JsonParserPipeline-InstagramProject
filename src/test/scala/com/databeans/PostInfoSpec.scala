package com.databeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Timestamp

case class PostInformation (is_comments_disabled: Boolean , height : Long , width : Long , display_url : String , likes : Long ,
                 count : Long , gating_info : String , id : String , is_video : Boolean ,
                 location : String , media_preview : String , username_id : String , shortcode : String ,
                 taken_at_timestamp : Timestamp , thumbnail_src : String , username : String)

class PostInfoSpec extends AnyFlatSpec with Matchers with GivenWhenThen{
  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("PostInfo builder test")
    .getOrCreate()

  val dataFrameSample = spark.read.option("multiline", true).json("TestedJson.json")
val postInfo = new PostInfo(spark)

  import spark.implicits._
  "buildPostInfoDataFrame" should "build the PostInfo DataFrame" in {
    Given("the json dataframe")
    val rawDataFrame = dataFrameSample
    When("buildPostInfoDataFrame should be invoked")
    val postInfoDataFrame = postInfo.buildPostInfoDataFrame(rawDataFrame)
    Then("PostInfo dataframe should be returned")
    val dateDataFrame = Seq(1613326858).toDF("time")
    val timeStampDataFrame = dateDataFrame.select(col("time").cast(TimestampType))
    val timeStampArray = timeStampDataFrame.collect().map(_.getTimestamp(0))
    val expectedDataFrame = Seq(
      PostInformation(false , 720 , 1080 , "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=6610cca9bb632966cade9c27cdfd3bf5&oe=60CBE492&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2-ccb7-4" ,
        692580 , 31 , null , "2509090007038408221" , false , null , null ,"1382894360" ,"CLSFBFTAbYd" , timeStampArray(0), "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c240.0.960.960a/s640x640/151035755_238172547987750_333496449389803091_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=VUx8EjdNI1gAX-XXbP3&edm=APU89FABAAAA&ccb=7-4&oh=0824aeded92582ee00295d07f98e636f&oe=60CAF7CB&_nc_sid=86f79a&ig_cache_key=MjUwOTA5MDAwMjMyMzk0Mjc5NQ%3D%3D.2.c-ccb7-4"
        , "phil.coutinho")
    ).toDF()
    postInfoDataFrame.collect() should contain theSameElementsAs(expectedDataFrame.collect())


  }

}
