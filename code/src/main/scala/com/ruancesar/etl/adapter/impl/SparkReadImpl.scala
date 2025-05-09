package com.ruancesar.etl.adapter.impl

import com.ruancesar.etl.adapter.SparkRead
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkReadImpl(spark: SparkSession) extends SparkRead {
  override def reads3(format: String, s3: String): DataFrame = {
    spark
      .read
      .format(format)
      .load(s3)
  }
}