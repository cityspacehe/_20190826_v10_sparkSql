package com.cityspace

import org.apache.spark.sql

/**
  *
  * @author hq
  * @date 2019-08-27
  *
  */
object MysqlTest extends App {

  val session=new sql.SparkSession.Builder()
    .master("local").appName(MysqlTest.getClass.getSimpleName)
    .getOrCreate()

  val sc=session.sparkContext
  sc.setLogLevel("WARN")

//  val url="jdbc:mysql://h01:3306/dept"
//  val table="dept"
//  val pro=new Properties()
//  pro.setProperty("user","root")
//  pro.setProperty("password","root")
//
//  val df=session.read.jdbc(url,table,pro)
//  df.show()

  val df=session.read.format("jdbc").options(
    Map(
      "url"->"jdbc:mysql://h01:3306/db1903",
      "user"->"root",
      "password"->"root",
      "dbtable"->"Emp"  // Emp  Dept
    )
  ).load()

//  df.show(2)

//  var flag=true
//  df.collect().foreach(row=>{
//    if(flag) {
//      row.schema.foreach(data => {
//        print(data.name + ":" + data.dataType.typeName + " ")
//      })
//      println(row.size)
//    }
//    println()
//    flag=false
//
//    for(i <-  0 until row.size) {
//      print(row.get(i) + " ")
//    }
//  })
//
//

//  df.describe("ename","sal","comm").show()
  // 数值型的字段求值

  // |summary|   ename|              sal|             comm|
  //+-------+--------+-----------------+-----------------+
  //|  count|      16|               16|                4|
  //|   mean|    null|           5502.5|           927.25|
  //| stddev|    null|1990.410275964899|568.9348966856107|
  //|    min|     Bob|           2380.0|            345.0|
  //|    max|zhangsan|           9794.0|           1565.0|
  //+-------+--------+-----------------+-----------------+

  println(df.first().mkString(","))

  df.where("sal>2500 and sal <5000").show()
  df.where("sal between 2500 and 5000").show()








}
