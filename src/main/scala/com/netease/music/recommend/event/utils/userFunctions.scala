package com.netease.music.recommend.event.utils

import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object userFunctions {

  // 较大跨度特征的离散化，如粉丝数
  def logDiscretize(maxSize:Int, logBase:Double) = udf((largeNum:Long) => {

    val initArray = new Array[Double](maxSize)

    if (largeNum > 0) {

      val pos = (math.log10(largeNum) / math.log10(logBase)).toInt
      initArray(if (pos < maxSize) pos else maxSize - 1) = 1

    }

    Vectors.dense(initArray).toSparse

  })

  // 分割字符串通用方法
  def splitString(sep:String) = udf((line:String) => {
    if (line == null || line.isEmpty) Array[String]() else line.split(sep)
  })

  // 提取下标对应内容
  def extractId(idPos:Int, sep:String):UserDefinedFunction = udf((videoInfo:String) => {

    val idString = videoInfo.split(sep)(idPos)

    idString.toLong

  })

  // 取向量的第一个数值
  def vectorHead = udf((x:ml.linalg.DenseVector) => x(1))


}
