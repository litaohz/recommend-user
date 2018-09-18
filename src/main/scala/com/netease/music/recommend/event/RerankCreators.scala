package com.netease.music.recommend.event

import java.io.InputStream

import com.netease.music.recommend.event.transform.tableTransform._
import com.netease.music.recommend.event.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object RerankCreators {

  def rankWithScore = udf((items:Seq[String]) => {

    items.map(line => {
      val arr = line.split("-_-")
      (arr(0), arr(1).toDouble)
    })
      .sortWith(_._2 > _._2)
      .slice(0, 50)
      .map(_._1)
      .mkString(",")

  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val options = new Options()
    options.addOption("matchResultInput", true, "positive/negative sample Input Directory")
    options.addOption("modelPath", true, "model path")
    options.addOption("creatorFeatureInput", true, "creator feature paths")
    options.addOption("userFeatureInput", true, "user feature paths")
    options.addOption("needTmpOutput", false, "if need save prediction")
    options.addOption("needFinalOutput", false, "if need save prediction")
    options.addOption("tmpOutput", true, "video part output directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val matchResultInput = cmd.getOptionValue("matchResultInput")
    val modelPath = cmd.getOptionValue("modelPath")
    val creatorFeatureInput = cmd.getOptionValue("creatorFeatureInput")
    val userFeatureInput = cmd.getOptionValue("userFeatureInput")
    val tmpOutput = cmd.getOptionValue("tmpOutput")
    val output = cmd.getOptionValue("output")

    // 读取parameters
    val stream : InputStream = this.getClass.getResourceAsStream("/parameters.json")
    implicit val formats = DefaultFormats

    val parameters = parse(stream)

    // 读取准备好的样本集
    val matchResult = spark.read.parquet(matchResultInput)

    // 召回结果变换
    val intermediateResult = mergeResultTransform(matchResult, spark)

    // 读取创作者特征仓库
    val creatorFeatureOrigin = spark.read.parquet(creatorFeatureInput.split(",") :_ *)

    // 创作者特征变换
    val creatorFeature = creatorFeatureTransform(creatorFeatureOrigin, spark, parameters)

    // 读取用户特征仓库
    val userFeatureOrigin = spark.read.parquet(userFeatureInput.split(",") :_ *)

    // 用户特征变换
    val userFeature = userFeatureOrigin

    // 交叉特征
    val dataset = mergeAllFeatureForPredict(intermediateResult, creatorFeature, userFeature, spark, parameters)

    // 预测分数
    val model = PipelineModel.load(modelPath)

    val prediction = model.transform(dataset)
      .withColumn("score", vectorHead($"probability"))

    if (cmd.hasOption("needTmpOutput")) {
      val saveColumns = Array("userId", "creatorInfo") ++ model.stages(0).asInstanceOf[VectorAssembler].getInputCols :+ "score"
      prediction.select(saveColumns.map(col): _ *)
        .write.parquet(tmpOutput)
    }

    val rankedResult = prediction
      .select("userId", "creatorInfo", "score")
      .withColumn("infoWithScore", concat_ws("-_-", $"creatorInfo", $"score"))
      .groupBy("userId")
      .agg(collect_list($"infoWithScore").as("result"))
      .withColumn("result", rankWithScore($"result"))
      .select("userId", "result")

    if (cmd.hasOption("needFinalOutput")) {
      rankedResult
        .write.option("sep", "\t").csv(output)
    }

  }

}
