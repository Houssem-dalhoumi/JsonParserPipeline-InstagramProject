package com.databeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.TimestampType
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Timestamp

case class commentsTable (username_id: String ,Post_id : String, created_at : Timestamp, data_id:String,
                          text:String, profile_pic_url:String, owner_id:String,usernameOfCommentator:String
                         )

class ProfileCommentsInfoSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("CommentsInfoTest")
    .getOrCreate()
  import spark.implicits._

  val dataFrameSample = spark.read.option("multiLine", true).json("TestedJson.json")
val profileCommentsInfo = new ProfileCommentsInfo(spark)
  "getCommentsData" should "explode the raw json file " in {
    Given("")
val rawDataframe = dataFrameSample
    When("getCommentsData is invoked")
    val commentsData = profileCommentsInfo.getCommentsData(rawDataframe)
Then("")
    val testedDataFrame = spark.read.option("multiline", true).json("OutputJson.json")
    val expextedDataFrame = testedDataFrame.select($"Post_id",$"Profile_id",explode($"col"))
    commentsData.collect() should contain theSameElementsAs expextedDataFrame.collect()
  }

  val utileFonctions = new UtileFonctions(spark)

  "buildCommentsDataFrame" should "build the Comments DataFrame" in {
    Given("The json DataFrame")
    val rawDataFrame = dataFrameSample
    When("buildCommentsDataFrame is invoked")
    val profileCommentsTable = profileCommentsInfo.buildCommentsDataFrame(rawDataFrame)
    Then("the Comments dataframe should be returned")
    val dateDataFrame = Seq(1619023981, 1614461897).toDF("time")
    val timeStampDataFrame = dateDataFrame.select(col("time").cast(TimestampType))
    val timeStampArray = timeStampDataFrame.collect().map(_.getTimestamp(0))

    val expectedCommentsDataTable =  Seq(
      commentsTable("1382894360" ,"2509090007038408221" , timeStampArray(0) , "18114517408211027"  , "🙏🏻 Deus não erra, não falha, Ele sabe de todas as coisas! 🙌🏻 Deus está no comando da sua vida e logo vc estará de volta aos campos com força total 🦵🏻 ⚽️ 🥅" ,
        "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/145618949_157498136023202_8971597364344501743_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=Kb6K0HyxiwgAX_xz_8v&edm=AI-cjbYBAAAA&ccb=7-4&oh=21bd0fcf2b127b5d38787edb2892715a&oe=60CC0D4B&_nc_sid=ba0005",
        "268668518" , "juliana_gilaberte" ),
      commentsTable("1382894360" , "2509090007038408221", timeStampArray(1) , "17995065436316161" , "🙌🙌🙌" , "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/120348221_3478281682231360_257373290020159950_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=mz6Idmuhx4IAX8iazcT&edm=AI-cjbYBAAAA&ccb=7-4&oh=aadacd17aae58fee0b22fd7d8e29669d&oe=60CB8502&_nc_sid=ba0005" ,
        "143817593" , "brunoitan" )
    ).toDF()

    profileCommentsTable.collect() should contain theSameElementsAs(expectedCommentsDataTable.collect())
  }



}
