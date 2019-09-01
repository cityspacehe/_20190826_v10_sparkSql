import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author hq
 * @date 2019-09-01
 */
public class BulkLoadDriver extends Configured implements Tool {
    private static Logger log;
    private static Configuration conf;
    private static Connection conn;
    private static final String INPUT_PATH="hdfs://h01:8020/hbase_input";
    private static final String OUTPUT_PATH="hdfs://h01:8020/hbase_output";


    static{
        try {
            log= Logger.getLogger(BulkLoadDriver.class);
            conf= HBaseConfiguration.create();
            conn= ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try {
            log.info(ToolRunner.run(new BulkLoadDriver(),args));

            Admin admin=conn.getAdmin();
            Table table=conn.getTable(TableName.valueOf("travel"));
            LoadIncrementalHFiles load=new LoadIncrementalHFiles(conf);
            load.doBulkLoad(new Path(OUTPUT_PATH),admin,table,
                    conn.getRegionLocator(TableName.valueOf("travel")));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Table table=conn.getTable(TableName.valueOf("travel"));

        Job job= Job.getInstance(conf);
        job.getConfiguration().set("mapred.jar",
                "/home/hadoop/TravelProject/out/artifacts/Travel/travel." +
                        "jar");  //预先将程序打包再将jar分发到集群上

        job.setJarByClass(BulkLoadDriver.class);
        job.setMapperClass(GenerateHfile.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setOutputFormatClass(HFileOutputFormat2.class);

        HFileOutputFormat2.configureIncrementalLoad(job,
                table,conn.getRegionLocator(TableName.valueOf("travel")));

        FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
        return job.waitForCompletion(true)?0:1;

    }


    private class GenerateHfile extends Mapper<LongWritable,
                Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            String[] str=value.toString().split(",");
            String ROWKEY=str[1]+str[2]+str[3];
            ImmutableBytesWritable rowkey=new ImmutableBytesWritable(ROWKEY.getBytes());
            Put put=new Put(ROWKEY.getBytes());
            put.addColumn("info".getBytes(),"url".getBytes(),str[0].getBytes());
            put.addColumn("info".getBytes(),"sp".getBytes(),str[1].getBytes()); // 出发地
            put.addColumn("info".getBytes(),"ep".getBytes(),str[2].getBytes());  //目的地
            put.addColumn("info".getBytes(),"st".getBytes(),str[3].getBytes());  // 出发时间
            put.addColumn("info".getBytes(),"price".getBytes(),str[4].getBytes()); // 价格
            put.addColumn("info".getBytes(),"traffic".getBytes(),str[5].getBytes()); // 交通方式
            put.addColumn("info".getBytes(),"hotel".getBytes(),str[5].getBytes()); // 酒店

            context.write(rowkey,put);
        }


        /**
         * HBase中每张Table在根目录（/HBase）下用一个文件夹存储，Table名为文件夹名，
         * 在Table文件夹下每个Region同样用一个文件夹存储，每个Region文件夹下的每个列族也用文件夹存储，
         * 而每个列族下存储的就是一些HFile文件，HFile就是HBase数据在HFDS下存储格式
         *
         *
         * 在put数据时会先将数据的更新操作信息和数据信息写入WAL，在写入到WAL后，
         * 数据就会被放到MemStore中，当MemStore满后数据就会被flush到磁盘(即形成HFile文件),
         * 在这过程涉及到的flush,split,compaction等操作都容易造成节点不稳定，
         * 数据导入慢，耗费资源等问题，在海量数据的导入过程极大的消耗了系统性能，
         * 避免这些问题最好的方法就是使用BlukLoad的方式来加载数据到HBase中。
         *
         *
         * 利用HBase数据按照HFile格式存储在HDFS的原理，
         * 使用Mapreduce直接生成HFile格式文件后，RegionServers再将HFile文件移动到相应的Region目录下
         *
         *
         * 注意
         * 1.Mapper的输出Key类型必须是包含Rowkey的ImmutableBytesWritable格式，Value类型必须为KeyValue或Put类型，当导入的数据有多列时使用Put，只有一个列时使用KeyValue
         * 2.job.setMapOutPutValueClass的值决定了job.setReduceClass的值，这里Reduce主要起到了对数据进行排序的作用，当job.setMapOutPutValueClass的值Put.class和KeyValue.class分别对应job.setReduceClass的PutSortReducer和KeyValueSortReducer
         * 3.在创建表时对表进行预分区再结合MapReduce的并行计算机制能加快HFile文件的生成，如果对表进行了预分区(Region)就设置Reduce数等于分区数（Region）
         * 4.在多列族的情况下需要进行多次的context.write
         *
         *
         *
         * 由于BulkLoad是绕过了Write to WAL，
         * Write to MemStore及Flush to disk的过程，所以并不能通过WAL来进行一些复制数据的操作
         *
         * 优点：
         * 1.导入过程不占用Region资源
         * 2.能快速导入海量的数据
         * 3.节省内存
         *
         */

    }
}






