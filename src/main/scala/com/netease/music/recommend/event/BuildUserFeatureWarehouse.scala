package com.netease.music.recommend.event

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

object BuildUserFeatureWarehouse {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val options = new Options()
    options.addOption("userVectorFromFollowInput", true, "preprocess video log input")
    options.addOption("dateStr", true, "event video relation input")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val userVectorFromFollowInput = cmd.getOptionValue("userVectorFromFollowInput")
    val dateStr = cmd.getOptionValue("dateStr")
    val output = cmd.getOptionValue("output")

    val userVectorFromFollow = spark.read.parquet(userVectorFromFollowInput)
      .select(
        $"userIdStr".cast(LongType).as("userId"),
        $"features".as("userVectorFromFollow")
      )

    val finalResult = userVectorFromFollow

    finalResult.write.parquet(output)

  }

}
