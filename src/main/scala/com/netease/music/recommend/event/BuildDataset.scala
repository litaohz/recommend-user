package com.netease.music.recommend.event

import java.io.InputStream

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.netease.music.recommend.event.utils.userFunctions._
import com.netease.music.recommend.event.utils.vectorFunctions._
import com.netease.music.recommend.event.transform.tableTransform._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{ArrayType, DoubleType}


object BuildDataset {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val options = new Options()
    options.addOption("sampleBasePath", true, "positive/negative sample Input Directory")
    options.addOption("sampleInput", true, "positive/negative sample Input Directory")
    options.addOption("creatorFeatureBasePath", true, "creator feature base path")
    options.addOption("creatorFeatureInput", true, "creator feature paths")
    options.addOption("userFeatureBasePath", true, "user feature base path")
    options.addOption("userFeatureInput", true, "user feature paths")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val sampleBasePath = cmd.getOptionValue("sampleBasePath")
    val sampleInput = cmd.getOptionValue("sampleInput")
    val creatorFeatureBasePath = cmd.getOptionValue("creatorFeatureBasePath")
    val creatorFeatureInput = cmd.getOptionValue("creatorFeatureInput")
    val userFeatureBasePath = cmd.getOptionValue("userFeatureBasePath")
    val userFeatureInput = cmd.getOptionValue("userFeatureInput")
    val output = cmd.getOptionValue("output")

    // 读取parameters
    val stream : InputStream = this.getClass.getResourceAsStream("/parameters.json")
    implicit val formats = DefaultFormats

    val parameters = parse(stream)

    // 读取准备好的样本集
    val samples = spark.read
      .option("basePath", sampleBasePath)
      .parquet(sampleInput.split(",") :_ *)

    // 读取创建者特征仓库
    val creatorFeatureOrigin = spark.read
      .option("basePath", creatorFeatureBasePath)
      .parquet(creatorFeatureInput.split(",") :_ *)

    // 读取所需参数
    val creatorFeature = creatorFeatureTransform(creatorFeatureOrigin, spark, parameters)

    // 读取用户特征仓库
    val userFeatureOrigin = spark.read
      .option("basePath", userFeatureBasePath)
      .parquet(userFeatureInput.split(",") :_ *)

    // 特征预变换
    val userFeature = userFeatureOrigin

    // 交叉特征
    val dataset = mergeAllFeature(samples, creatorFeature, userFeature, spark, parameters)

    dataset.write.parquet(output)

  }

}
