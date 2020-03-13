package net.hanbd.telecom.analysis.mr.kv;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 总计信息
 *
 * @author hanbd
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Summation implements Writable {
    private int callSum;
    private int callDurationSum;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(callSum);
        dataOutput.writeInt(callDurationSum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.callSum = dataInput.readInt();
        this.callDurationSum = dataInput.readInt();
    }
}
