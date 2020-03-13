package net.hanbd.telecom.analysis.mr.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * dimension标记接口
 *
 * @author hanbd
 */
public abstract class BaseDimension implements WritableComparable<BaseDimension> {

    @Override
    public abstract int compareTo(BaseDimension o);

    @Override
    public abstract void write(DataOutput dataOutput) throws IOException;

    @Override
    public abstract void readFields(DataInput dataInput) throws IOException;
}
