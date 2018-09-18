package com.netease.music.recommend.event

import java.io.InputStream

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object TrainLRModel {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val options = new Options()
    options.addOption("datasetInput", true, "positive/negative sample Input Directory")
    options.addOption("modelOutput", true, "positive/negative sample Input Directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val datasetInput = cmd.getOptionValue("datasetInput")
    val modelOutput = cmd.getOptionValue("modelOutput")
    val output = cmd.getOptionValue("output")

    // 载入配置
    val stream : InputStream = this.getClass.getResourceAsStream("/model_config.json")
    implicit val formats = DefaultFormats

    val modelName = "baseline"
    val modelConfig = parse(stream) \ modelName

    // assembler
    val assemblerInputCols = (modelConfig \ "assembler" \ "inputCols").extract[Array[String]]
    val assemblerOutputCol = (modelConfig \ "assembler" \ "outputCol").extract[String]

    val assembler = new VectorAssembler()
      .setInputCols(assemblerInputCols)
      .setOutputCol(assemblerOutputCol)

    // minMaxScaler
    val minMaxScaler = new MinMaxScaler()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("features")

    // LogisticRegression
    val maxIter = (modelConfig \ "lr" \ "maxIter").extract[Int]
    val regParam = (modelConfig \ "lr" \ "regParam").extract[Double]

    val lr = new LogisticRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)

    // pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, minMaxScaler, lr))

    // 读取训练数据集
    val dataset = spark.read.parquet(datasetInput)

    val trainTestSplit = dataset.randomSplit(Array(0.8, 0.2))
    val trainingDataset = trainTestSplit(0)
    val testDataset = trainTestSplit(1)

    // 训练
    val pipelineModel = pipeline.fit(trainingDataset)

    pipelineModel.write.overwrite().save(modelOutput)

    // 验证
    val predictions = pipelineModel.transform(testDataset)
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")

    val auc = evaluator.evaluate(predictions)
    println("areaUnderROC = " + auc)

  }

}
