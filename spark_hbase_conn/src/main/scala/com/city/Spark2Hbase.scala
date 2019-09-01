package com.city

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-09-01
  *
  */
object Spark2Hbase extends App {

  val session=SparkSession.builder().master("local")
    .appName("nfnb").getOrCreate()
  session.sparkContext.setLogLevel("WARN")

  val conf=HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum",
    "192.168.130.111:2181,192.168.130.112:2181," +
      "192.168.130.113:2181")

  val tableName="student"
  val conn=ConnectionFactory.createConnection(conf)
  val table=conn.getTable(TableName.valueOf(tableName))

  conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
  val job = Job.getInstance(conf)
  job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
  job.setMapOutputValueClass (classOf[KeyValue])

  HFileOutputFormat2.configureIncrementalLoadMap(job,table)

//  val num = session.sparkContext.parallelize(1 to 10)
//  val rdd = num.map(f=>{
//   val kv= new KeyValue("apark%03d".format(f).getBytes(),"info".getBytes(),"name".getBytes(),s"lisi${f}".getBytes())
//    (new ImmutableBytesWritable(Bytes.toBytes(f)), kv)
//  })
//
//  // Save Hfiles on HDFS
//  rdd.saveAsNewAPIHadoopFile("hdfs://192.168.130.112:8020/tmp/hbase", classOf[ImmutableBytesWritable], classOf[KeyValue],
//    classOf[HFileOutputFormat2], conf)

  //Bulk load Hfiles to Hbase
  val bulkLoader = new LoadIncrementalHFiles(conf)
  bulkLoader.doBulkLoad(new Path("hdfs://192.168.130.112:8020/tmp/hbase")
    ,conn.getAdmin,table,conn.getRegionLocator(TableName.valueOf(tableName)))

}
