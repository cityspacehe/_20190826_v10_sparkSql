import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
  *
  * @author hq
  * @date 2019-08-31
  *
  */
object HdfsTest extends App {
  val cfg=new Configuration()
  cfg.set("fs.defaultFS","hdfs://192.168.130.112:8020")
  cfg.set("dfs.replication","1")
  // 也可将文件 core-site.xml 和 hdfs-site.xml 文件放在resource 下面

  // cfg.addResource("core-site.xml")

  val fs=FileSystem.get(cfg)
  println(fs.exists(new Path("/spark")))
  fs.deleteOnExit(new Path( "/spark_spark"))



}
