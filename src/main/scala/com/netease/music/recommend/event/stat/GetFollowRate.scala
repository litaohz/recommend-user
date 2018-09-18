package com.netease.music.recommend.event.stat

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._

object GetFollowRate {

  def computeSmoothRate(alpha:Double, beta:Int) = udf((clickCount:Long, impressCount:Long) => {

    (clickCount.toDouble + alpha) / (impressCount + beta)

  })

  /**
    * 统计添加朋友和动态卡片两个场景的关注数量
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
    options.addOption("baseStatBasePath", true, "user action base path input")
    options.addOption("baseStatInput", true, "user action base path input")
    options.addOption("dateStr", true, "event video relation input")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val baseStatBasePath = cmd.getOptionValue("baseStatBasePath")
    val baseStatInput = cmd.getOptionValue("baseStatInput")
    val dateStr = cmd.getOptionValue("dateStr")
    val output = cmd.getOptionValue("output")

    val baseStat = spark.read.option("basePath", baseStatBasePath).parquet(baseStatInput.split(",") :_ *)

    val naValueMap = Map(
      "addFollowImpressCount" -> 0,
      "addFollowFollowCount" -> 0,
      "eventCardImpressCount" -> 0,
      "eventCardFollowCount" -> 0
    )

    /**
      * 关注率平滑数据依据
      * scala> res12.filter($"addFollowImpressCount" > 200).withColumn("followRate", $"addFollowFollowCount" / $"addFollowImpressCount").describe("followRate").show
      * +-------+--------------------+
      * |summary|          followRate|
      * +-------+--------------------+
      * |  count|                4861|
      * |   mean|0.004143066624627...|
      * | stddev|0.007398292162708494|
      * |    min|                 0.0|
      * |    max| 0.13259195893926431|
      * +-------+--------------------+
      *
      *
      * scala> res12.filter($"eventCardImpressCount" > 200).withColumn("followRate", $"eventCardFollowCount" / $"eventCardImpressCount").describe("followRate").show
      * +-------+-------------------+
      * |summary|         followRate|
      * +-------+-------------------+
      * |  count|               5663|
      * |   mean| 0.0130663656297386|
      * | stddev|0.01895752980688395|
      * |    min|                0.0|
      * |    max|0.22127659574468084|
      * +-------+-------------------+
      */

    val followRate = baseStat
      .na.fill(naValueMap)
      .groupBy("creatorId")
      .agg(
        sum("addFollowImpressCount").as("addFollowImpressCount7d"),
        sum("addFollowFollowCount").as("addFollowFollowCount7d"),
        sum("eventCardImpressCount").as("eventCardImpressCount7d"),
        sum("eventCardFollowCount").as("eventCardFollowCount7d")
      )
      .withColumn("addFollowFollowRate7d", computeSmoothRate(1, 320)($"addFollowFollowCount7d", $"addFollowImpressCount7d"))
      .withColumn("eventCardFollowRate7d", computeSmoothRate(1, 120)($"eventCardFollowCount7d", $"eventCardImpressCount7d"))

    followRate.write.parquet(output)

  }

}
