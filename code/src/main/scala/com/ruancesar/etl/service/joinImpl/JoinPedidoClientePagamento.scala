package com.ruancesar.etl.service.joinImpl

import com.ruancesar.etl.service.impl._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class JoinPedidoClientePagamento(df1: DataFrame, df2: DataFrame) {
  def join(): DataFrame = {
    val dfjoin1 = df1.join(df2, Seq("order_id"))
    dfjoin1
  }
}
