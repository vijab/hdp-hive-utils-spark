package com.vijai.hive

import org.apache.spark.sql.SparkSession
import com.hortonworks.hwc.HiveWarehouseSession

trait HiveSession {

  def withSimpleHiveContext(sessionBuilder: SparkSession.Builder): SparkSession =
    sessionBuilder.enableHiveSupport()
    .getOrCreate()


  def withWarehouseConnector(sessionBuilder: SparkSession.Builder): SparkSession = {
    HiveWarehouseSession.session(withSimpleHiveContext(sessionBuilder))
      .build().session()
  }


}