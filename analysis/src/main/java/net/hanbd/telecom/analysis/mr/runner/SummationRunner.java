package net.hanbd.telecom.analysis.mr.runner;

import lombok.Cleanup;
import lombok.SneakyThrows;
import net.hanbd.telecom.analysis.mr.kv.CombineDimension;
import net.hanbd.telecom.analysis.mr.kv.Summation;
import net.hanbd.telecom.analysis.mr.mapper.SummationMapper;
import net.hanbd.telecom.analysis.mr.output.MysqlOutputFormat;
import net.hanbd.telecom.analysis.mr.reducer.SummationReducer;
import net.hanbd.telecom.analysis.util.PropertyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * @author hanbd
 */
public class SummationRunner implements Tool {
    private Configuration conf;

    public static SummationRunner getInstance() {
        return new SummationRunner();
    }

    @Override
    public int run(String[] strings) throws Exception {
        // 实例化job
        Job job = Job.getInstance(this.conf);
        job.setJarByClass(SummationRunner.class);
        // 组装Mapper InputFormat
        initHbaseInputConfig(job);
        // 组装Reducer OutputFormat
        initReducerOutputConfig(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @SneakyThrows
    private void initHbaseInputConfig(Job job) {
        @Cleanup Connection conn = ConnectionFactory.createConnection(conf);
        @Cleanup Admin admin = conn.getAdmin();

        // 检查表是否存在
//        String tableName = PropertyUtil.getProperty("hbase.tableName");
        String tableName = "telecom:calllog";
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            throw new RuntimeException("Couldn't find hbase table: " + tableName);
        }
        // 定义扫描器
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                SummationMapper.class,
                CombineDimension.class,
                IntWritable.class,
                job
        );
    }

    private void initReducerOutputConfig(Job job) {
        job.setReducerClass(SummationReducer.class);
        job.setOutputKeyClass(CombineDimension.class);
        job.setOutputValueClass(Summation.class);
        job.setOutputFormatClass(MysqlOutputFormat.class);
    }
}
