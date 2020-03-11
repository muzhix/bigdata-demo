package net.hanbd.telecom.consumer.hbase;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import net.hanbd.telecom.consumer.util.HbaseUtil;
import net.hanbd.telecom.consumer.util.PropertyUtil;

import java.util.List;
import java.util.Optional;

/**
 * @author hanbd
 */
public class CalleeWriteObserver implements RegionObserver, RegionCoprocessor {
    private static final String TARGET_TABLE = PropertyUtil.getProperty("hbase.calllog.tableName");
    private static final Splitter SPLITTER = Splitter.on("_");
    private static final String COLUMN_FAMILY = "f1";

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @SneakyThrows
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) {
        String currentTable = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
        if (!TARGET_TABLE.equals(currentTable)) {
            return;
        }

        String rowKey = Bytes.toString(put.getRow());
        List<String> keys = Lists.newArrayList(SPLITTER.split(rowKey));
        String caller = keys.get(1);
        String callee = keys.get(2);
        String establishTime = keys.get(3);
        String duration = keys.get(4);
        String calleeFlag = "0";

        int regionNum = Integer.parseInt(PropertyUtil.getProperty("hbase.calllog.regionNum"));
        String regionCode = HbaseUtil.genRegionCode(caller, establishTime, regionNum);
        String calleeRowKey = HbaseUtil.genRowKey(regionCode, callee, caller, establishTime, duration, calleeFlag);

        Put calleePut = HbaseUtil.genPut(COLUMN_FAMILY, calleeRowKey, caller, callee, establishTime, duration,
                calleeFlag);
        @Cleanup Table table = c.getEnvironment().getConnection().getTable(TableName.valueOf(TARGET_TABLE));
        table.put(calleePut);
    }
}
