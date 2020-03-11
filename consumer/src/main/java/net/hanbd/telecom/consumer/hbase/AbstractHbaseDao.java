package net.hanbd.telecom.consumer.hbase;

import org.apache.hadoop.conf.Configuration;

/**
 * @author hanbd
 */
public abstract class AbstractHbaseDao {
    protected String namespace;
    protected String tableName;
    protected int regionNum;
    protected Configuration conf;

    public AbstractHbaseDao(String namespace, String tableName, int regionNum, Configuration conf) {
        this.namespace = namespace;
        this.tableName = tableName;
        this.regionNum = regionNum;
        this.conf = conf;
    }

    /**
     * 向表中写入数据
     *
     * @param value 数据
     */
    public abstract void put(Object value);
}
