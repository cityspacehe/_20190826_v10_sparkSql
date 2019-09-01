package com

import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-08-31
  *
  */
object Spark_HiveTest extends App {
  val session=SparkSession.builder().master("local")
    .appName("use").getOrCreate()

  session.read.format("jdbc").options(
    Map(
      "driver"->"org.apache.hive.jdbc.HiveDriver",
      "user"->" ",
      "password"->"",
      "url"->"jdbc:hive2://192.168.130.111:10000/works",
      "dbtable"->"score"
    )).load().show


}
