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
 * 混合维度，包括用户维度与时间维度
 *
 * @author hanbd
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CombineDimension extends BaseDimension {
    private UserDimension userDimension = new UserDimension();
    private DateDimension dateDimension = new DateDimension();

    @Override
    public int compareTo(BaseDimension dimen) {
        CombineDimension that = (CombineDimension) dimen;
        return ComparisonChain.start()
                .compare(this.dateDimension, that.dateDimension)
                .compare(this.userDimension, that.userDimension).result();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        userDimension.write(dataOutput);
        dateDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userDimension.readFields(dataInput);
        this.dateDimension.readFields(dataInput);
    }
}
