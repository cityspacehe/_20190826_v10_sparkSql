import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  *
  * @author hq
  * @date 2019-08-31
  *
  */
object Spark_write2_Hbase02 extends App {

  // 获取sparkSql 连接对象
  val session = SparkSession.builder().master("local")
    .appName("nfnb").getOrCreate()
  session.sparkContext.setLogLevel("WARN")

  val sc = session.sparkContext

  val jobCfg = new JobConf()
  // 获取hbase 操作对象
  jobCfg.set("hbase.zookeeper.quorum",
    "192.168.130.111:2181,192.168.130.112:2181," +
      "192.168.130.113:2181")
  // hbase 连接必须在本地添加映射文件
  // conf.set("zookeeper.znode.parent","/hbase-unsecure"); // 出错无法解决的时候必须加
  jobCfg.setOutputFormat(classOf[TableOutputFormat])
  jobCfg.set(TableOutputFormat.OUTPUT_TABLE, "student")

  // 一分区用map 否则用mappartitons
//  sc.parallelize(1 to 20, 5).map(
//    t => {
//      val put = new Put(s"st${t}".getBytes())
//      put.addColumn("info".getBytes(),"name".getBytes(),s"stu${t}".getBytes())
//      put.addColumn("info".getBytes(),"age".getBytes(),s"${t+20}".getBytes())
//      put.addColumn("info".getBytes(),"sex".getBytes(),s"${if(t%2==0) "men" else "women" }".getBytes())
//      (new ImmutableBytesWritable(),put)
//  }).saveAsHadoopDataset(jobCfg)

  // 没有输出
  type t=ImmutableBytesWritable
  sc.parallelize(1 to 20, 5).mapPartitions(f=>{
   val list=List[(t,Put)]()
    f.foreach(t => {
      val put = new Put(s"mm${t}".getBytes())
      put.addColumn("info".getBytes(),"name".getBytes(),s"stu${t}".getBytes())
      put.addColumn("info".getBytes(),"age".getBytes(),s"${t+20}".getBytes())
      put.addColumn("info".getBytes(),"sex".getBytes(),s"${if(t%2==0) "men" else "women" }".getBytes())
      list :+ (new ImmutableBytesWritable(),put)
    })
    list.iterator
  }).saveAsHadoopDataset(jobCfg)





}
