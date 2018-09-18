package com.netease.music.recommend.event.utils

import breeze.linalg.DenseVector
import org.apache.spark.ml
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object vectorFunctions {

  // 向量相乘
  def vecDot:UserDefinedFunction = udf((vec0:Seq[Double], vec1:Seq[Double]) => {

    if (vec0 == null || vec1 == null) {
      0.0
    } else {
      val bv0 = new DenseVector(vec0.toArray)
      val bv1 = new DenseVector(vec1.toArray)
      bv0 dot bv1
    }

  })

  /**
    * 如果这个特征向量为null，填充一个全为0，长度为size的向量
    * @param defaultValue 被填充的默认向量
    * @return
    */
  def fillNAVector(defaultValue:ml.linalg.Vector):UserDefinedFunction = udf((input:ml.linalg.Vector) => {

    if (input != null) {
      input
    } else {
      defaultValue
    }
  })

}
