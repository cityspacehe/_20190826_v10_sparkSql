import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
/**
  *
  * @author hq
  * @date 2019-08-31
  *
  */
object Spark_write2_Hbase03 extends App {
  // 获取操作hdfs 对象
  val conf=new Configuration()
  conf.set("fs.defaultFS","hdfs://192.168.130.112:/8020")
  conf.set("dfs.replication","1")
  val fs=FileSystem.get(conf)
//  fs.mkdirs(new Path("/spark"))
//  fs.deleteOnExit(new Path("/spark"))

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

  val job=Job.getInstance(cfg)
  job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setMapOutputValueClass(classOf[KeyValue])

  HFileOutputFormat2.configureIncrementalLoadMap(job,htable)

  type ty=ImmutableBytesWritable
  val rdd=session.sparkContext.parallelize(1 to 11,1)
    .map(f=>{
 //     val kv=new KeyValue("apark%03d".format(f).getBytes(),"info".getBytes(),"name".getBytes(),s"lisi${f}".getBytes())
     val kv=new KeyValue(s"00${f+20}}".getBytes(),"info".getBytes(),"name".getBytes(),s"lisi${f}".getBytes())
      (new ty(),kv)
    })
  // KeyValue(s"spark%03d".format(f).getBytes,

  // 将rdd转换成Hfile并存储在HDFS上
   val path="hdfs://192.168.130.112:8020/tmp/hbasee"
//  // 路径存在则删除
  val pa=new Path(path)
  if (fs.exists(pa)) {
    fs.delete(pa,true)
  }
  rdd.saveAsNewAPIHadoopFile(path,classOf[ty],classOf[KeyValue],
    classOf[HFileOutputFormat2],cfg)

  // 将HFile 添加在hbase中
  // 获取 student 表的所在的region
  var regionLactor=conn.getRegionLocator(htable.getName)

  var bulkLoader=new LoadIncrementalHFiles(cfg)
  bulkLoader.doBulkLoad(
    new Path("hdfs://ljr:8020/tmp/hbasee/"),
    conn.getAdmin,htable,regionLactor)

}

/**
  * f"dd${f}.03d".getBytes()
  * hbase 添加的时候 会对 rowkey 进行字典排序  11 应该在2 的前面
  * 直接添加在hfile 的时候  hbase 已经无法对其进行排序   所以会出现错误
  *
  * 词法上  lexically
  *
  * java.io.IOException: Added a key not lexically larger
  * than previous. Current cell
  * = spark10/info:name/1567154297750/Put/vlen=8/seqid=0,
  * lastCell = spark9/info:name/1567154297750/Put/vlen=7/seqid=0
  *
// 以下这个错误式由于   "fs.defaultFS","hdfs://192.168.130.112:/8020"

   原因在于集合搭建的时候不对  core-site.xml

  * 2019-09-01 09:46:18,349 ERROR [org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles] - Encountered unrecoverable error from region server, additional details: row '' on table 'student' at region=student,,1567250188234.4f6b4bf72a4014bd63a98538cab25bee., hostname=hdp01,16020,1567235464388, seqNum=2
  * org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed after attempts=35, exceptions:
  * Sun Sep 01 09:37:06 CST 2019, RpcRetryingCaller{globalStartTime=1567301826871, pause=100, retries=35}, java.io.IOException: java.io.IOException: Wrong FS: hdfs://192.168.130.112:8020/tmp/hbasee/info/6e59d33ae60e445db54825de331542a1, expected: hdfs://ljr
  * at org.apache.hadoop.hbase.ipc.RpcServer.call(RpcServer.java:2434)
  * at org.apache.hadoop.hbase.ipc.CallRunner.run(CallRunner.java:123)
  * at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:188)
  * at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:168)
  * Caused by: java.lang.IllegalArgumentException: Wrong FS: hdfs://192.168.130.112:8020/tmp/hbasee/info/6e59d33ae60e445db54825de331542a1, expected: hdfs://ljr
  * at org.apache.hadoop.fs.FileSystem.checkPath(FileSystem.java:643)
  * at org.apache.hadoop.hdfs.DistributedFileSystem.getPathName(DistributedFileSystem.java:184)
  * at org.apache.hadoop.hdfs.DistributedFileSystem.access$000(DistributedFileSystem.java:101)
  * at org.apache.hadoop.hdfs.DistributedFileSystem$17.doCall(DistributedFileSystem.java:1068)
  * at org.apache.hadoop.hdfs.DistributedFileSystem$17.doCall(DistributedFileSystem.java:1064)
  * at org.apache.hadoop.fs.FileSystemLinkResolver.resolve(FileSystemLinkResolver.java:81)
  * at org.apache.hadoop.hdfs.DistributedFileSystem.getFileStatus(DistributedFileSystem.java:1064)
  * at org.apache.hadoop.fs.FilterFileSystem.getFileStatus(FilterFileSystem.java:397)
  * at o
  *
  */