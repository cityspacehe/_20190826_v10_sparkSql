package com.cityspace

import org.apache.spark.sql

/**
  *
  * @author hq
  * @date 2019-08-27
  *
  */
object HiveTest extends App {

  val session=new sql.SparkSession.Builder().master("local")
    .appName(HiveTest.getClass.getSimpleName)
    .enableHiveSupport()  //  启动hive  这种方式是通过配置文件连接hive
    .getOrCreate()

 // session.sql("create database d1903")
//  session.sql("show databases").show()
  session.sql("select * from tmp.dept").show

  //
//  val conf = new SparkConf()
//  conf.setAppName("WordCount").setMaster("local")
//  val hive =SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
//  hive.sql("show tables")
//
//
//  val df=session.read.format("jdbc").options(
//    Map(
//      "driver"->"org.apache.hive.jdbc.HiveDriver",
//      "url"->"jdbc:hive2://h02:10000/tmp",
//      "user"->"hdfs",
//      "password"->"hdfs",
//      "dbtable"->"dept"  // Emp  Dept
//    )).load().show


}

