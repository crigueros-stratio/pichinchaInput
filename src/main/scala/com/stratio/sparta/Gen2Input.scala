/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.sparta

import com.stratio.sparta.sdk.lite.batch.models._
import com.stratio.sparta.sdk.lite.xd.batch._
import org.apache.spark.sql._
import org.apache.spark.sql.crossdata.XDSession

class Gen2Input(
                 xdSession: XDSession,
                 properties: Map[String, String]
               )
  extends LiteCustomXDBatchInput(xdSession, properties) {

  val filesystem: String = properties("filesystem")
  val accountName: String = properties("accountName")
  val file: String = properties("file")

  /**
   * parametro para csv
   */
  val formato: String = properties("formato")
  val header: String = properties("header")
  val delimiter: String = properties("delimiter")

  override def init(): ResultBatchData = {

    if (formato.toUpperCase == "CVS") {
      val rddFrame: DataFrame = xdSession.read
        .option("header",header)
        .option("delimiter", delimiter)
        .csv("abfss://" + filesystem + "@" + accountName + ".dfs.core.windows.net/" + file)
      ResultBatchData(rddFrame.rdd)
    } else {
      val rddFrame: DataFrame = xdSession.read.parquet("abfss://" + filesystem + "@" + accountName + ".dfs.core.windows.net/" + file)
      ResultBatchData(rddFrame.rdd)
    }


  }
}
