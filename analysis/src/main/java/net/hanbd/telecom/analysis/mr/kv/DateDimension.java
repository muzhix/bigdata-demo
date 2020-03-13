package net.hanbd.telecom.analysis.mr.kv;

import com.google.common.collect.ComparisonChain;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 时间维度
 *
 * @author hanbd
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DateDimension extends BaseDimension {
    private int year;
    private int month;
    private int day;

    @Override
    public int compareTo(BaseDimension dimen) {
        DateDimension that = (DateDimension) dimen;
        return ComparisonChain.start()
                .compare(this.year, that.year)
                .compare(this.month, that.month)
                .compare(this.day, that.day)
                .result();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
    }


}
