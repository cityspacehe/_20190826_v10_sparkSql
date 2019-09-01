import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.sql.SparkSession


/**
  *
  * @author hq
  * @date 2019-08-31
  *
  *
  *       put 'student','ts001','info:age','12'
  *       put 'student','ts001','info:name','zhangsan'
  *       put 'student','ts001','info:sex','man'
  *put 'student','ts002','info:age','14'
  */
object Spark_HbaseTest01 extends App {
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
  val admin=conn.getAdmin

  cfg.set(TableInputFormat.INPUT_TABLE,"student")

  val rdd=session.sparkContext.newAPIHadoopRDD(cfg,
    classOf[TableInputFormat],classOf[ImmutableBytesWritable],
    classOf[Result])
    .cache()

  import session.implicits._

  // 每一个f 循环的是每个主键所对应的所有信息
  val df=rdd.map(f=>{
    // 只会获取最后一条的主键  因此没有意义
    println(new String(f._1.get()))
    println("----------------------------------------")

    // 获取本次主键及其所有列的信息
    val result=f._2
    println(s"${new String(result.getRow)}")


    // 获取主键
    val rowkey=Bytes.toString(result.getRow)


     val cells=result.listCells()
    println("---ghgfjjnj-"+cells.size())

   // 这样的方式要保证每个rowkey对应的字段数目一样  不然会造成数组下标越界
//    val name=Bytes.toString(CellUtil.cloneValue(cells.get(0)))
//    val age=Bytes.toString(CellUtil.cloneValue(cells.get(1)))
//    val sex=Bytes.toString(CellUtil.cloneValue(cells.get(2)))
//    (rowkey,name,age,sex)



    // 可以代替上面的方式

    for(i <- 0 until cells.size()){
      val cell=cells.get(i)
      // 获取值有以下方法  只有最后一种不会出现乱码的现象
      println(new String(cell.getValueArray))
      println(Bytes.toString(cell.getValueArray))
      println(Bytes.toString(CellUtil.cloneValue(cell)))
    }




//    这样的方式可以将字段和值一一对应   并且可以获取空字段
    val map=new java.util.HashMap[String,String]()
    val scan=result.cellScanner()
    while(scan.advance()){
      val cell=scan.current()
      val cName=Bytes.toString(CellUtil.cloneQualifier(cell))
      val cValue=Bytes.toString(CellUtil.cloneValue(cell))
      map.put(cName,cValue)
    }
    (rowkey,map.get("name"),map.get("age"),map.get("sex"))

  }).toDF("id","name","age","sex")

  df.createOrReplaceTempView("tb_student")

  session.sql("select *from tb_student").show(100)


}
