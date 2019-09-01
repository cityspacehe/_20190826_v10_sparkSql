package com.cityspace


import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  *
  * @author hq
  * @date 2019-08-28
  *
  */
object UDAFTest extends App {
  val session = new sql.SparkSession.Builder().master("local")
    .appName(UDAFTest.getClass.getSimpleName).getOrCreate()

  val sc = session.sparkContext
  sc.setLogLevel("WARN")

  import session.implicits._

  sc.parallelize(List("zhangsan" -> 18, "lisi" -> 34, "wangwuwu" -> 33))
    .toDF("name", "age").createOrReplaceTempView("tb_user")

  session.udf.register("cityMax", new CityUDAF)
  session.sql("select cityMax(name) from tb_user").show
  //session.sql("select name from tb_user").show

}

class CityUDAF extends UserDefinedAggregateFunction {
  // 定义输入类型
  override def inputSchema: StructType = StructType(
    Array(StructField("name", StringType)))

  // 定义中间缓存类型
  override def bufferSchema: StructType = StructType(
    Array(StructField("ch", StringType), StructField("num", IntegerType))
  )

  // 定义输出类型
  override def dataType: DataType = StringType

  // 输入和输出是否同类型
  override def deterministic: Boolean = true

  // 初始化中间缓存对象
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
    buffer(1) = 0
  }

  // 分区内合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val name = input.getString(0)
    //    println("============================="+name)
    val array = new Array[Integer](26)
    for (ch <- name.toLowerCase.toCharArray) {
      if (array(ch - 'a') == null) {
        array(ch - 'a') = 1
      } else {
        array(ch - 'a') += 1
      }
    }
    var max: Integer = 0
    var index = 0

    for (i <- 0 to array.length - 1) {
      if (array(i) != null && array(i) > max) {
        max = array(i)
        index = i
      }
    }

    buffer(0) = (index + 'a').toChar.toString
    buffer(1) = max

  }

  // 分区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val b1 = buffer1.getAs[Integer](1)
    val b2 = buffer2.getAs[Integer](1)
    if (b1 > b2) {
      buffer1(0) = buffer1.getAs[String](0)
      buffer1(1) = b1
    } else {
      buffer1(0) = buffer2.getAs[String](0)
      buffer1(1) = b2
    }
  }

  // 结果
  override def evaluate(buffer: Row): Any = {
    (buffer.getString(0) -> buffer.getInt(1)).toString()
  }
}


object rt extends App {
  val array = new Array[Integer](26)
  val t = "zhanhhhhgsaaaaaaan"

  for (ch <- t.toLowerCase.toCharArray) { // 122 -97
    //    println(ch.toInt)
    //    println(array.length)
    //    println(array.size)
    if (array(ch - 'a') == null) {
      array(ch - 'a') = 1
    } else {
      array(ch - 'a') += 1
    }
  }
  var max = array(0)
  var index = 0
  for (i <- 1 to array.length - 1) {
    if (array(i) != null && array(i) > max) {
      max = array(i)
      index = i
    }
  }
  println("heejj " + (index + 'a').toChar + "  " + max)


  //
  //  println("----max "+array.maxBy(_!=null)) // 满足条件的第一个值
  //  val max=array.maxBy(_!=null)
  //  var index=0;
  //
  //  for(i <- 0 to array.length-1){
  //    if(array(i)==max){
  //      index=i
  //    }
  //  }
  //  println("max  "+(index+'a').toChar+"  "+max)


  for (i <- 0 to array.length - 1) {
    if (array(i) != null) {
      println((i + 'a').toChar + " " + array(i))
    }
  }
}

object tu extends App {
  var arr = Array(12, 23, 34, 4)
  for (i <- 0 to arr.length - 1) {
    arr(i) += 1
    println(arr(i))
  }
  for (i <- arr) {
    println(i)
  }
}
