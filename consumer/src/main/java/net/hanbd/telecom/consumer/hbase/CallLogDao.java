package net.hanbd.telecom.consumer.hbase;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import net.hanbd.telecom.consumer.util.HbaseUtil;
import net.hanbd.telecom.consumer.util.PropertyUtil;

import java.util.List;

/**
 * @author hanbd
 */
public class CallLogDao extends AbstractHbaseDao {
    private static final String CALLER_COLUMN_FAMILY = "f1";
    private static final String CALLER_FLAG = "1";
    private static final String CALLEE_COLUMN_FAMILY = "f2";
    private static final String CALLEE_FLAG = "0";
    private static final int MAX_CACHE_SIZE = 30;

    private static final Configuration CALLLOG_CONF = HBaseConfiguration.create();
    private static final Splitter SPLITTER = Splitter.on(",");

    private List<Put> cacheList = Lists.newArrayList();

    public CallLogDao() {
        super(PropertyUtil.getProperty("hbase.calllog.namespace"),
                PropertyUtil.getProperty("hbase.calllog.tableName"),
                Integer.parseInt(PropertyUtil.getProperty("hbase.calllog.regionNum")),
                CALLLOG_CONF);
    }

    public static CallLogDao getInstance() {
        return new CallLogDao();
    }

    @SneakyThrows
    @Override
    public void put(@NonNull Object value) {
        String strVal = String.valueOf(value);
        List<String> strList = Lists.newArrayList(SPLITTER.split(strVal));

        String caller = strList.get(0);
        String callee = strList.get(1);
        String establishTime = strList.get(2);
        String duration = strList.get(3);

        String regionCode = HbaseUtil.genRegionCode(caller, establishTime, this.regionNum);
        String rowKey = HbaseUtil.genRowKey(regionCode, caller, callee, establishTime, duration, CALLER_FLAG);
        Put callerPut = HbaseUtil.genPut(CALLER_COLUMN_FAMILY, rowKey, caller, callee, establishTime, duration,
                CALLER_FLAG);

        /**
         * calleePut TODO 将此部分改为协处理器 {@link CalleeWriteObserver}
         */
        String calleeRowKey = HbaseUtil.genRowKey(regionCode, callee, caller, establishTime, duration, CALLEE_FLAG);
        Put calleePut = HbaseUtil.genPut(CALLEE_COLUMN_FAMILY, calleeRowKey, caller, callee, establishTime, duration,
                CALLEE_FLAG);

        cacheList.add(callerPut);
        cacheList.add(calleePut);

        if (cacheList.size() > MAX_CACHE_SIZE) {
            @Cleanup Table table = getTable();
            table.put(cacheList);
            cacheList.clear();
        }
    }

    @SneakyThrows
    private Table getTable() {
        return HbaseUtil.getConnection(this.conf).getTable(TableName.valueOf(this.tableName));
    }
}
