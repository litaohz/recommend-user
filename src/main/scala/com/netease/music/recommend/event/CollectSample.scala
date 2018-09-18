package com.netease.music.recommend.event

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object CollectSample {

  def isBefore = udf((impressTime:Long, followTime:Long) => {

    if (followTime > impressTime) {
      1
    } else {
      0
    }

  })

  /**
    * 收集用户卡片页面的正负样本
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val options = new Options()
    options.addOption("biUserActionInput", true, "biUserAction input")
    options.addOption("dateStr", true, "event video relation input")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val biUserActionInput = cmd.getOptionValue("biUserActionInput")
    val dateStr = cmd.getOptionValue("dateStr")
    val output = cmd.getOptionValue("output")

    //    val biUserActionInput = "/user/da_music/hive/warehouse/music_dw.db/user_action/dt=2018-07-24,/user/da_music/hive/warehouse/music_dw.db/user_action/dt=2018-07-23,/user/da_music/hive/warehouse/music_dw.db/user_action/dt=2018-07-22,/user/da_music/hive/warehouse/music_dw.db/user_action/dt=2018-07-21,/user/da_music/hive/warehouse/music_dw.db/user_action/dt=2018-07-20,/user/da_music/hive/warehouse/music_dw.db/user_action/dt=2018-07-19,/user/da_music/hive/warehouse/music_dw.db/user_action/dt=2018-07-18"

    val allUserActionDay = spark.read.parquet(biUserActionInput)

    // 全量关注行为
    val followTotal = allUserActionDay
      .filter($"action" === "follow")
      .select($"userId", $"props.id".cast(LongType).as("creatorId"), $"logTime".as("followTime"))
      .groupBy("userId", "creatorId")
      .agg(max($"followTime").as("followTime"))
      .cache

//    // 添加关注页的曝光和关联关注
//    val addFollowImpress = allUserActionDay
//      .filter($"action" === "impress" && $"props.page" === "addfollow" && $"props.target" === "follow")
//      .select($"userId", $"props.resourceid".cast(LongType).as("creatorId"), $"logTime".as("ImpressTime"))
//      .groupBy("userId", "creatorId")
//      .agg(min($"impressTime").as("impressTime"))
//
//    val addFollowData = addFollowImpress
//      .join(followTotal, Seq("userId", "creatorId"), "left_outer")
//      .withColumn("oweTo", isBefore($"impressTime", $"followTime"))
//      .groupBy("creatorId")
//      .agg(count("impressTime").as("addFollowImpressCount"), sum("oweTo").as("addFollowFollowCount"))

    // 添加动态流用户的曝光和关联关注
    val eventCardImpress = allUserActionDay
      .filter($"action" === "impress" && $"props.page" === "eventpage" && $"props.target" === "user_card" && $"props.resource" === "rcmmd_user")
      .select($"userId", $"props.targetid".cast(LongType).as("creatorId"), $"logTime".as("ImpressTime"))
      .groupBy("userId", "creatorId")
      .agg(min($"impressTime").as("impressTime"))

    val eventCardData = eventCardImpress
      .join(followTotal, Seq("userId", "creatorId"), "left_outer")
      .withColumn("label", isBefore($"impressTime", $"followTime"))
      .na.fill(0, Seq("label"))
      .filter($"userId" > 0)

    // 合并和保存最终结果
    val finalResult = eventCardData

    finalResult.write.parquet(output)


  }

}
