import lombok.Cleanup;
import lombok.CustomLog;
import lombok.SneakyThrows;
import net.hanbd.telecom.consumer.hbase.CalleeWriteObserver;
import net.hanbd.telecom.consumer.util.HbaseUtil;
import net.hanbd.telecom.consumer.util.PropertyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Test;

public class CalllogInit {
    private static final Configuration conf = HBaseConfiguration.create();
    private String tableName = PropertyUtil.getProperty("hbase.calllog.tableName");
    private int regionNum = Integer.parseInt(PropertyUtil.getProperty("hbase.calllog.regionNum"));
    private String[] columnFamilies = {"f1", "f2"};


    public void createTable() {
        HbaseUtil.createTable(conf, tableName, regionNum, columnFamilies);
    }

    @SneakyThrows
    public void dynamicAddCoprocessor() {
        @Cleanup Admin admin = HbaseUtil.getAdmin(conf);
        TableName tableName = TableName.valueOf(this.tableName);
        TableDescriptorBuilder table = TableDescriptorBuilder.newBuilder(tableName);

        Path path = new Path("hdfs://centos:9000/hbase/coprocessor/CallLogCoprocessor.jar");
        CoprocessorDescriptorBuilder cpDescBuilder = CoprocessorDescriptorBuilder
                .newBuilder(CalleeWriteObserver.class.getCanonicalName())
                .setJarPath(path.toString())
                .setPriority(Coprocessor.PRIORITY_USER);

        table.setCoprocessor(cpDescBuilder.build());
        for (String cf : columnFamilies) {
            table.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf));
        }

        admin.disableTable(tableName);
        admin.modifyTable(table.build());
        admin.enableTable(tableName);
    }

    @Test
    public void create() {
        createTable();
//        dynamicAddCoprocessor();
    }

    @Test
    public void test(){
        System.out.println(CalleeWriteObserver.class.getCanonicalName());
    }
}
