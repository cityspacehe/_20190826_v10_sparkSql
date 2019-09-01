import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-08-31
  *
  * spark 添加hbase 数据方式一
  */
object Sprk_Write2_hbase01 extends App {
  // 获取操作hdfs 对象
  val conf=new Configuration()
  conf.set("fs.defaultFS","hdfs://192.168.130.112:/8020")
  conf.set("dfs.replication","1")
  val fs=FileSystem.get(conf)
  fs.mkdirs(new Path("/spark"))
  fs.deleteOnExit(new Path("/spark"))

  // 获取sparkSql 连接对象
  val session=SparkSession.builder().master("local")
    .appName("nfnb").getOrCreate()
  session.sparkContext.setLogLevel("WARN")

  // 获取hbase 操作对象
  val cfg=HBaseConfiguration.create()
  cfg.set("hbase.zookeeper.quorum",
    "192.168.130.111:2181,192.168.130.112:2181," +
      "192.168.130.113:2181")
  // hbase 连接必须在本地添加映射文件
  // conf.set("zookeeper.znode.parent","/hbase-unsecure"); // 出错无法解决的时候必须加
  val conn=ConnectionFactory.createConnection(cfg)

  val htable=conn.getTable(TableName.valueOf("student"))
  val list=new util.ArrayList[Put]()

  for(i <- 1 to 10){
    val put =new Put(s"ts${i+10}".getBytes()) // 主键
    put.addColumn("info".getBytes(),"name".getBytes(),s"stu${i}".getBytes())
    put.addColumn("info".getBytes(),"age".getBytes(),s"${i+20}".getBytes())
    put.addColumn("info".getBytes(),"sex".getBytes(),s"${if(i%2==0) "men" else "women" }".getBytes())
    list.add(put)
  }
  htable.put(list)


}
