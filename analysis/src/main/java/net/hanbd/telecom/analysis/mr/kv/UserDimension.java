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
 * 用户维度
 *
 * @author hanbd
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserDimension extends BaseDimension {
    private String phone;
    private String name;

    @Override
    public int compareTo(BaseDimension dimen) {
        UserDimension that = (UserDimension) dimen;
        return ComparisonChain.start()
                .compare(this.phone, that.phone)
                .compare(this.name, that.name).result();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.name = dataInput.readUTF();
    }
}
