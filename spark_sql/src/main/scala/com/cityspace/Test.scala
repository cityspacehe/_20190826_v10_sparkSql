package com.cityspace

import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-08-26
  *
  */
object Test extends App{

  // val sc=new SparkContext(new SparkConf().setMaster("local").setAppName(Test.getClass.getSimpleName))
  val session=SparkSession.builder().master("local")
    .appName(Test.getClass.getSimpleName)
    .getOrCreate()
  val sc=session.sparkContext
  sc.setLogLevel("WARN")

  case class Student(name:String,age:Int)  // 样本类 ，自动实现tostring equal apply 方法

  val peopleRDD=sc.textFile(System.getProperty("user.dir") +"/spark_sql/inpath/peopleInfo")
    .map(line=>{
      val arrs=line.split(",")
      Student(arrs(0),arrs(1).trim.toInt)
    })

  val df=session.createDataFrame(peopleRDD).createOrReplaceTempView("tb_user")

  session.sql("select *from tb_user").show()

//  val df1=session.createDataFrame(peopleRDD).toDF() //  可以指定字段名  toDF("name","age)等
//  df1.show

}

