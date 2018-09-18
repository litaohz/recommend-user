package com.netease.music.recommend.event

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object BuildCreatorFeatureWarehouse {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val options = new Options()
    options.addOption("userFollowCountInput", true, "user follow count input")
    options.addOption("eventMeta90dInput", true, "preprocess video log input")
    options.addOption("followRateInput", true, "preprocess video log input")
    options.addOption("creatorVectorFromFollowInput", true, "preprocess video log input")
    options.addOption("dateStr", true, "event video relation input")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val userFollowCountInput = cmd.getOptionValue("userFollowCountInput")
    val eventMeta90dInput = cmd.getOptionValue("eventMeta90dInput")
    val followRateInput = cmd.getOptionValue("followRateInput")
    val creatorVectorFromFollowInput = cmd.getOptionValue("creatorVectorFromFollowInput")
    val dateStr = cmd.getOptionValue("dateStr")
    val output = cmd.getOptionValue("output")

    // 创作者粉丝数
    val userFollowCount = spark.read.option("sep", "\t")
      .csv(userFollowCountInput)
      .select(
        $"_c0".cast(LongType).as("creatorId"),
        $"_c1".cast(LongType).as("followCount")
      )

    // 创作者近期原创动态数
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(sdf.parse(dateStr))
    cal.add(Calendar.DAY_OF_YEAR, -90)
    val timeLimit = cal.getTimeInMillis

    val validEventType = Seq(18, 13, 17, 39)

    val userEventCount = spark.read.json(eventMeta90dInput)
      .filter($"Type".isin(validEventType :_ *))
      .filter($"EventTime" > timeLimit)
      .groupBy("UserId")
      .count
      .withColumnRenamed("UserId", "creatorId")
      .withColumnRenamed("count", "publishedEventCount")
    
    // 创作者在推荐用户场景的关注率
    val followRate = spark.read.parquet(followRateInput)

    // 创作者向量
    val creatorVectorFromFollow = spark.read.parquet(creatorVectorFromFollowInput)
      .select(
        $"friendIdStr".cast(LongType).as("creatorId"),
        $"features".as("creatorVectorFromFollow")
      )

    // 合并创作者特征
    val finalResult = userFollowCount
      .join(userEventCount, Seq("creatorId"), "outer")
      .join(followRate, Seq("creatorId"), "outer")
      .join(creatorVectorFromFollow, Seq("creatorId"), "outer")

    finalResult.write.parquet(output)

  }

}
