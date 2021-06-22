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

class Gen2Input2(
                                     xdSession: XDSession,
                                     properties: Map[String, String]
                                   )
  extends LiteCustomXDBatchInput(xdSession, properties) {

  override def init(): ResultBatchData = {
    val rddFrame = funciones.downloadFiles(xdSession, properties)
    ResultBatchData(rddFrame.rdd)
  }
}
