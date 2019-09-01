package cityspace

import java.text.SimpleDateFormat

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-08-29
  *
  */
object HiveOperator extends App {
  val session=new sql.SparkSession.Builder().master("local")
    .appName(HiveOperator.getClass.getSimpleName)
    .enableHiveSupport().getOrCreate()

  session.sparkContext.setLogLevel("WARN")
//  session.sql("drop table spark.tb_emp")
//  session.sql("drop database spark")
//  HiveUtil.init(session)
 // HiveUtil.loadData(session)

  HiveUtil.ETLData(session)
  /**
    * 1.创建数据库（首次）
    * 2.创建表（首次）
    * 每日
    * 3.导入数据（分区）
    * 4.ETL
    * 5.导出数据到新表
    *
    *
    */

}
object HiveUtil{

  def init(session:SparkSession)={
    // 初始化数据库
    session.sql("drop database if exists spark")
    session.sql("create database if not exists spark")
    session.sql("use spark")
    // 创建表
    session.sql("drop table if exists tb_emp")
    session.sql("create external table if not exists tb_emp" +
      "(empno int,ename string,job string,mgr int,hdate string,sal double," +
      "comm double,deptno int)partitioned by (hdates int,hday int)" +
      "row format delimited fields terminated by ','" +
      "location 'hdfs://192.168.1.128:8020/apps/hive/warehouse/spark.db'")
  }

  def loadData(session: SparkSession)={
    import java.util.Date
    val date=new Date()
    val dateformat=new SimpleDateFormat("yyyyMM,dd")
    val dateStr=dateformat.format(date).split(",")
    session.sql(s"load data inpath 'hdfs://192.168.1.128:8020/spark_inpath/tb_tmp' into table spark.tb_emp partition(hdates=${dateStr(0)},hday=${dateStr(1)})")
  }

  def ETLData(session: SparkSession)={
    val tb_emp_df=session.sql(s"select max(sal) maxsal,min(sal) as minsal,job,${System.currentTimeMillis()} htime from spark.tb_emp group by job")
    tb_emp_df.write.saveAsTable("spark.tb_emp_info")
  }




}
