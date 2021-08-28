package com.databeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Timestamp
case class profileInformation ( created_time : Timestamp, username :String , biography : String, followers_count : Long,
                     following_count: Long , full_name: String ,
                     username_id :String , is_business_account:Boolean , is_joined_recently:Boolean ,
                     is_private:Boolean , posts_count :Long,
                     profile_pic_url : String)

class ProfileInfoSpec extends AnyFlatSpec with Matchers with GivenWhenThen{
  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("ProfileInfoTest")
    .getOrCreate()

  val dataFrameSample = spark.read.option("multiline", true).json("TestedJson.json")
  val profileInfo = new ProfileInfo(spark)

  import spark.implicits._

  "buildProfileInfoDataFrame" should "create ProfileInfo DataFrame" in {
  Given("the json dataframe")
  val rawDataFrame = dataFrameSample

  When("buildProfileInfoDataFrame is invoked ")
  val profileInfoDataFrame = profileInfo.buildProfileInfoDataFrame(rawDataFrame)

  Then("ProfileInfo dataframe should be returned ")
  val dateDataFrame = Seq(1286323200).toDF("time")
  val timeStampDataFrame = dateDataFrame.select(col("time").cast(TimestampType))
  val timeStampArray = timeStampDataFrame.collect().map(_.getTimestamp(0))
  val expectedDataFrame = Seq(
    profileInformation ( timeStampArray(0), "phil.coutinho", "", 23156762, 1092, "Philippe Coutinho", "1382894360", false, false, false, 618,
      "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/69437559_363974237877617_991135940606951424_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=uiYY_up9lLwAX8rG9wR&edm=ABfd0MgBAAAA&ccb=7-4&oh=c3a24d2609c83e4cf8d017318f3b034e&oe=60CBC5C0&_nc_sid=7bff83")
  ).toDF()

  profileInfoDataFrame.collect() should contain theSameElementsAs(expectedDataFrame.collect())
}


}
