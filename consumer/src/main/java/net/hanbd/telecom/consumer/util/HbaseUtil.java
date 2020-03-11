package net.hanbd.telecom.consumer.util;

import com.google.common.base.Joiner;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author hanbd
 */
public class HbaseUtil {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static Connection conn;

    /**
     * 获得hbase连接
     *
     * @param config 配置
     * @return Connection
     */
    @SneakyThrows
    public static synchronized Connection getConnection(Configuration config) {
        if (conn == null || conn.isClosed()) {
            conn = ConnectionFactory.createConnection(config);
        }
        return conn;
    }

    @SneakyThrows
    public static Admin getAdmin(Configuration conf) {
        return getConnection(conf).getAdmin();
    }

    /**
     * 创建namespace
     *
     * @param conf      配置
     * @param namespace 命名空间名称
     */
    @SneakyThrows
    public static void createNameSpace(String namespace, Configuration conf) {
        @Cleanup Admin admin = getAdmin(conf);
        NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(nd);
    }

    /**
     * 创建表
     *
     * @param conf           hadoop配置
     * @param tableName      表名
     * @param regionNum      预分区个数
     * @param columnFamilies 列族
     */
    @SneakyThrows
    public static void createTable(Configuration conf, String tableName, int regionNum, String... columnFamilies) {
        if (isTableExists(tableName, conf)) {
            return;
        }
        @Cleanup Admin admin = getAdmin(conf);

        TableDescriptorBuilder table = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String cf : columnFamilies) {
            table.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf));
        }

        admin.createTable(table.build(), genSplitKeys(regionNum));
    }

    /**
     * 生成分区键
     *
     * @param regionNum 分区个数
     * @return splitKeys
     */
    private static byte[][] genSplitKeys(int regionNum) {
        byte[][] splitKeys = new byte[regionNum][];

        // 格式化分区键 eg: 01| 02|
        String[] keys = new String[regionNum];
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regionNum; i++) {
            keys[i] = df.format(i) + "|";
        }

        // 保证分区有序
        TreeSet<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regionNum; i++) {
            set.add(Bytes.toBytes(keys[i]));
        }

        Iterator<byte[]> iterator = set.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            byte[] next = iterator.next();
            splitKeys[index++] = next;
        }

        return splitKeys;
    }

    /**
     * 生成分区号
     *
     * @param caller        主叫电话
     * @param establishTime 通话建立时间
     * @param regionNum     分区数
     * @return regionCode
     */
    public static String genRegionCode(String caller, String establishTime, int regionNum) {
        int len = caller.length();

        // 取出主叫后四位
        String last4 = caller.substring(len - 4);
        // 取时分秒
        String replace = establishTime.replaceAll(":", "").trim();
        String hms = replace.substring(replace.length() - 6);

        // 离散操作
        int hash = Integer.hashCode(Integer.parseInt(last4) ^ Integer.parseInt(hms));
        // 生成分区号
        int regionCode = hash % regionNum;

        return new DecimalFormat("00").format(regionCode);
    }

    /**
     * 生成rowKey: regionCode_caller_callee_establishTime_duration_flag
     *
     * @param regionCode    分区键
     * @param caller        主叫
     * @param establishTime 通话创建时间
     * @param callee        被叫
     * @param flag          标签
     * @param duration      通话时间
     * @return rowKey
     */
    public static String genRowKey(String regionCode, String caller, String callee,
                                   String establishTime, String duration, String flag) {
        Joiner joiner = Joiner.on("_").skipNulls();
        return joiner.join(regionCode, caller, callee, establishTime, duration, flag);
    }

    /**
     * 生成时间戳
     *
     * @param time 格式：yyyy-MM-dd HH:mm:ss
     * @return timestamp
     */
    public static String genTimestamp(String time) {
        long ts = LocalDateTime.parse(time, TIME_FORMATTER).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return String.valueOf(ts);
    }

    /**
     * 生成hbase Put对象
     *
     * @param columnFamily  列族
     * @param rowKey        rowKey
     * @param caller        主叫人号码
     * @param callee        被叫人号码
     * @param establishTime 通信建立时间
     * @param duration      持续时间
     * @param flag          标志位
     * @return net.hanbd.telecom.consumer.hbase Put
     */
    public static Put genPut(String columnFamily, String rowKey, String caller, String callee, String establishTime,
                             String duration,
                             String flag) {
        // 时间戳
        String establishTimeTs = HbaseUtil.genTimestamp(establishTime);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("caller"), Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("callee"), Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("establishTime"), Bytes.toBytes(establishTime));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("establishTimeTS"), Bytes.toBytes(establishTimeTs));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("flag"), Bytes.toBytes(flag));

        return put;
    }

    /**
     * 判断hbase表是否存在
     *
     * @param tableName 表名
     * @param conf      hadoop配置项
     * @return boolean
     */
    @SneakyThrows
    public static boolean isTableExists(String tableName, Configuration conf) {
        @Cleanup Admin admin = getAdmin(conf);
        return admin.tableExists(TableName.valueOf(tableName));
    }


}
