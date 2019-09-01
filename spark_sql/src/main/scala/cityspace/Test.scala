package cityspace

import org.apache.spark.sql

/**
  *
  * @author hq
  * @date 2019-08-29
  *
  */
object Test extends App {
  val session=new sql.SparkSession.Builder().master("local")
    .appName(Test.getClass.getSimpleName).getOrCreate()
}
