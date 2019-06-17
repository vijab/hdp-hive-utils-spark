package com.vijai.main

import com.vijai.hive.HiveSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

object DataPersisterMain extends HiveSession with Logging {

  case class Listing(id: Int, price: Double, title: String)

  val spark: SparkSession = withWarehouseConnector(SparkSession.builder()
      .master("local[4]")
      .appName("DataPersisterApp")
      //.config("spark.sql.warehouse.dir", "/tmp/spark_warehouse_dir")
      //.config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")
      .config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://localhost:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
  )

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS listings (id INT, price DOUBLE, title STRING)")

    val listings = spark.createDataset(
      (1 to 1000).map(i => Listing(
        Random.nextInt(i * 10),
        Random.nextInt(50000).toDouble,
        Random.alphanumeric.take(50).mkString(""))
      )
    )

    listings.toDF().write.mode(SaveMode.Overwrite)
      .saveAsTable("default.listings")

    sql("SELECT * FROM default.listings").show()

    log.info("Done writing listings.")

    spark.stop()
  }

}
