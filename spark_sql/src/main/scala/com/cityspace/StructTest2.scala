package com.cityspace

import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
/**
  *
  * @author hq
  * @date 2019-08-27
  *
  */

object StructTest2 extends App {
  val session=new spark.sql.SparkSession.Builder().master("local")
    .appName(StructTest2.getClass.getSimpleName).getOrCreate()

  val sc=session.sparkContext
  sc.setLogLevel("WARN")

  val rdd=sc.textFile(System.getProperty("user.dir")+"/spark_sql/inpath/student_info")
    .map(line=>{
      val arr=line.split(",")
      if(arr.length==3){
        Row(arr(0),arr(1).trim.toInt,arr(2))
      }else{
        Row(arr(0),arr(1).trim.toInt,null)
      }
    })

  val rowStruct=new StructType(Array(
    StructField("name",StringType,true),
    StructField("age",IntegerType,true),
    StructField("sex",StringType,true)  // 指定该字段可不可以为null  false-->不可以
  ))

  // 创建Struct来定义结构
  val  structType:StructType=StructType(
    StructField("name",StringType,true)::StructField("age",IntegerType,true)::StructField("sex",StringType,true)::Nil
  )

  session.createDataFrame(rdd,rowStruct).createOrReplaceTempView("tb_user")
  session.createDataFrame(rdd,structType).createOrReplaceTempView("tb_users")

  session.sql("select *from tb_user").show
  session.sql("select * from tb_users").show


}
