import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author hq
 * @date 2019-09-01
 */
public class Spark2HbaseUtil01 {
    private static  Configuration hbaseConf;
    private static  SparkSession sparkSession;
    private static SparkContext sc;
    static{
        hbaseConf= HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum",
                "192.168.130.111:2181,192.168.130.112:2181," +
                        "192.168.130.113:2181");

        sparkSession=SparkSession.builder().master("local").appName("vfjn").getOrCreate();
        sc=sparkSession.sparkContext();
    }

    public static void main(String[] args) {

    }
}
/**
 * Bulkload过程主要包括三部分：
 * 1.从数据源(通常是文本文件或其他的数据库)提取数据并上传到HDFS
 * 这一步不在HBase的考虑范围内，不管数据源是什么，
 * 只要在进行下一步之前将数据上传到HDFS即可。
 * 2.利用一个MapReduce作业准备数据
 * 这一步需要一个MapReduce作业，并且大多数情况下还需要我们自己编写Map函数，
 * 而Reduce函数不需要我们考虑，由HBase提供。该作业需要使用rowkey(行键)作为输出Key，
 * KeyValue、Put或者Delete作为输出Value。
 * MapReduce作业需要使用HFileOutputFormat2来生成HBase数据文件。
 * 为了有效的导入数据，需要配置HFileOutputFormat2使得每一个输出文件都在一个合适的区域中
 * 。为了达到这个目的，MapReduce作业会使用Hadoop的TotalOrderPartitioner
 * 类根据表的key值将输出分割开来。HFileOutputFormat2的方法
 * configureIncrementalLoad()会自动的完成上面的工作。
 * 3.告诉RegionServers数据的位置并导入数据
 * 这一步是最简单的，通常需要使用LoadIncrementalHFiles
 * (更为人所熟知是completebulkload工具)，将文件在HDFS上的位置传递给它，
 * 它就会利用RegionServer将数据导入到相应的区域。
 *
 */
