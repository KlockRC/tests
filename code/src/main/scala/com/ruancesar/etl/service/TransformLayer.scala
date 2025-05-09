package com.ruancesar.etl.service


import org.apache.spark.sql.DataFrame

trait TransformLayer {
  def getValidRecords(): DataFrame
  def getInvalidRecords(): DataFrame
}