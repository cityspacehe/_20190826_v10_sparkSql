package com.cityspace

import java.util.Random

import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-08-29
  *
  */
object DataTongjiTest extends App {
  val session=SparkSession.builder().master("local")
    .appName("fb").getOrCreate()   // 只能在写 rdd.toDF() 之后能导入

  import session.implicits._

  val sc=session.sparkContext
  sc.setLogLevel("WARN")
  val rdd=sc.parallelize(Range(0,10,step = 1))

  val df=rdd.toDF("id")

val ran=new Random()
  import org.apache.spark.sql.functions._
 // import org.apache.spark.sql.functions._  此包不可以少
  val d1=df.withColumn("rand1",rand(seed=10))
    .withColumn("rand2",rand(27))

  // 求相关系数 -0.1099396246708271
  println(d1.stat.corr("rand1", "rand2", "pearson"))
  // -0.1099396246708271
  println(d1.stat.corr("rand1", "rand2"))


}
