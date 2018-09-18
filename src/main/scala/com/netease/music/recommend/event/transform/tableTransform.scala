package com.netease.music.recommend.event.transform

import com.netease.music.recommend.event.utils.userFunctions._
import com.netease.music.recommend.event.utils.vectorFunctions.{fillNAVector, vecDot}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

object tableTransform {

  def creatorFeatureTransform(df: DataFrame, spark: SparkSession, parameters: JValue): DataFrame = {

    import spark.implicits._
    implicit val formats = DefaultFormats

    // 读取所需参数
    val addFollowAlpha = (parameters \ "smooth_parameter" \ "follow_rate_in_addfollow" \ "alpha").extract[Int]
    val addFollowBeta = (parameters \ "smooth_parameter" \ "follow_rate_in_addfollow" \ "beta").extract[Int]
    val eventCardAlpha = (parameters \ "smooth_parameter" \ "follow_rate_in_eventcard" \ "alpha").extract[Int]
    val eventCardBeta = (parameters \ "smooth_parameter" \ "follow_rate_in_eventcard" \ "beta").extract[Int]

    println("addFollowAlpha = ", addFollowAlpha)
    println("addFollowBeta = ", addFollowBeta)
    println("eventCardAlpha = ", eventCardAlpha)
    println("eventCardBeta = ", eventCardBeta)

    // 特征预变换
    val creatorFeature = df
      .withColumn("discreteFollowNum", logDiscretize(7, 10)($"followCount"))
      .withColumn("discretePublishedEventCount", logDiscretize(10, 2)($"publishedEventCount"))

    creatorFeature

  }

  def mergeResultTransform(df:DataFrame, spark:SparkSession): DataFrame = {

    import spark.implicits._

    df.withColumn("creatorInfo", explode(splitString(",")($"match")))
      .drop("match")
      .withColumnRenamed("userid", "userId")
      .withColumn("creatorId", extractId(0, ":")($"creatorInfo"))

  }


  def mergeAllFeature(intermediateResult:DataFrame,
                      creatorFeature:DataFrame,
                      userFeature:DataFrame,
                      spark: SparkSession,
                      parameters: JValue): DataFrame = {

    import spark.implicits._
    implicit val formats = DefaultFormats

    // 读取所需参数
    val addFollowAlpha = (parameters \ "smooth_parameter" \ "follow_rate_in_addfollow" \ "alpha").extract[Int]
    val addFollowBeta = (parameters \ "smooth_parameter" \ "follow_rate_in_addfollow" \ "beta").extract[Int]
    val eventCardAlpha = (parameters \ "smooth_parameter" \ "follow_rate_in_eventcard" \ "alpha").extract[Int]
    val eventCardBeta = (parameters \ "smooth_parameter" \ "follow_rate_in_eventcard" \ "beta").extract[Int]

    // 配置na值
    val naValueMap = Map(
      "addFollowFollowRate7d" -> (addFollowAlpha.toDouble / addFollowBeta),
      "eventCardFollowRate7d" -> (eventCardAlpha.toDouble / eventCardBeta)
    )

    // 开始交叉
    val dataset = intermediateResult
      .join(creatorFeature, Seq("creatorId", "dt"), "left_outer")
      .join(userFeature, Seq("userId", "dt"), "left_outer")
      .withColumn("alsFollowScore", vecDot($"userVectorFromFollow".cast(ArrayType(DoubleType, true)), $"creatorVectorFromFollow".cast(ArrayType(DoubleType, true))))
      .withColumn("discreteFollowNum", fillNAVector(Vectors.sparse(7, Array(0), Array(1.0)))($"discreteFollowNum"))
      .withColumn("discretePublishedEventCount", fillNAVector(Vectors.sparse(10, Array(0), Array(1.0)))($"discretePublishedEventCount"))
      .na.fill(naValueMap)

    dataset

  }

  def mergeAllFeatureForPredict(intermediateResult:DataFrame,
                      creatorFeature:DataFrame,
                      userFeature:DataFrame,
                      spark: SparkSession,
                      parameters: JValue): DataFrame = {

    import spark.implicits._
    implicit val formats = DefaultFormats

    // 读取所需参数
    val addFollowAlpha = (parameters \ "smooth_parameter" \ "follow_rate_in_addfollow" \ "alpha").extract[Int]
    val addFollowBeta = (parameters \ "smooth_parameter" \ "follow_rate_in_addfollow" \ "beta").extract[Int]
    val eventCardAlpha = (parameters \ "smooth_parameter" \ "follow_rate_in_eventcard" \ "alpha").extract[Int]
    val eventCardBeta = (parameters \ "smooth_parameter" \ "follow_rate_in_eventcard" \ "beta").extract[Int]

    // 配置na值
    val naValueMap = Map(
      "addFollowFollowRate7d" -> (addFollowAlpha.toDouble / addFollowBeta),
      "eventCardFollowRate7d" -> (eventCardAlpha.toDouble / eventCardBeta)
    )

    // 开始交叉
    val dataset = intermediateResult
      .join(creatorFeature, Seq("creatorId"), "left_outer")
      .join(userFeature, Seq("userId"), "left_outer")
      .withColumn("alsFollowScore", vecDot($"userVectorFromFollow".cast(ArrayType(DoubleType, true)), $"creatorVectorFromFollow".cast(ArrayType(DoubleType, true))))
      .withColumn("discreteFollowNum", fillNAVector(Vectors.sparse(7, Array(0), Array(1.0)))($"discreteFollowNum"))
      .withColumn("discretePublishedEventCount", fillNAVector(Vectors.sparse(10, Array(0), Array(1.0)))($"discretePublishedEventCount"))
      .na.fill(naValueMap)

    dataset

  }

}
