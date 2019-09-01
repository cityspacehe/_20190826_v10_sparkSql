package com.cityspace

import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-08-27
  *
  */
object JsonTest extends App {
  val session=new SparkSession.Builder().master("local").appName(JsonTest.getClass.getSimpleName)
    .getOrCreate()
  val sc=session.sparkContext
  sc.setLogLevel("WARN")
  session.read.json(System.getProperty("user.dir")+"/spark_sql/inpath/dept.json")
    .createOrReplaceTempView("dept")
//  df1.show()

  session.read.json(System.getProperty("user.dir")+"/spark_sql/inpath/emplotee.json")
      .createOrReplaceTempView("emp")
//  df2.show()

//  val dep=session.read.json(System.getProperty("user.dir")+"/spark_sql/inpath/dept.json")
//  val emp=session.read.json(System.getProperty("user.dir")+"/spark_sql/inpath/emplotee.json")
//
//  emp.join(dep,"id").groupBy(dep("name")).agg(avg(emp("salary")),).show()



 session.sql("select *from dept join emp on dept.id=emp.depID").show()


}
