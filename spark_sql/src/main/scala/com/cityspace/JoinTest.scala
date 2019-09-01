package com.cityspace


import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  *
  * @author hq
  * @date 2019-08-29
  *
  */
object JoinTest extends App {
  val session=new sql.SparkSession.Builder().master("local")
    .appName(JoinTest.getClass.getSimpleName)
    .getOrCreate()

  session.sparkContext.setLogLevel("WARN")





 case class Grade(salLow:Double,salHigh:Double,salLevel:String)

 val r1= session.sparkContext.textFile(System.getProperty("user.dir")+"/spark_sql/input/salgrade")
    .filter(_.length>0)
    .filter(_.split(",").length==3)
    .map(line=>{
      val arr=line.split(",")
      Grade(arr(0).toDouble,arr(1).toDouble,arr(2))
    })
  val df1=session.createDataFrame(r1) //员工薪资等级


  val r2=session.sparkContext.textFile(System.getProperty("user.dir")+"/spark_sql/input/emp")
    .filter(_.length>0)
    .filter(_.split(",").length==5)
    .map(line=>{
      val arr=line.split(",")
      Row(arr(0),arr(1),arr(2),arr(3).toDouble,arr(4).trim.toInt)
    })

  val rowStruct=new StructType(Array(
    StructField("empno",StringType,false),
    StructField("ename",StringType,false),
    StructField("job",StringType,false),
    StructField("sal",DoubleType,false),
    StructField("deptno",IntegerType,false)
  ))

  val df2=session.createDataFrame(r2,rowStruct) // 员工表

  val df3=session.read.json(System.getProperty("user.dir")+"/spark_sql/input/dept.json") // 部门表


//  df2.join(df3,"deptno").where("sal>5000").show()  // join的字段名相同

//  df2.join(df3,df2("dept")===df3("deptno")).select("empno","sal","deptno").show() //join字段名不同

// df2.select(df2("ename"),df2("dept"),df2("sal"),df2("sal")+1000).show // 所查询的字段都要用df()形式

 // df2.select(df2.apply("ename"),df2.col("sal")+1000,df2("sal")).show()

  // 以下三种方式都是获取到column 对象
  val e=df2.apply("ename")
  val e2=df2("ename")
  val e3=df2.col("ename")

  df2.select(df2.apply("ename"),df2.col("sal")+1000,df2("sal"),df2("deptno"))
    .orderBy(df2("deptno").desc,df2("sal")).show()  // 先按照部门降序排列，再按照工资升序


  df2.groupBy(df2.col("deptno")).max("sal").show() // 只能查询分组字段 和聚合字段






}
