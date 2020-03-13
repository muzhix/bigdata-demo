import com.google.common.collect.Lists;
import net.hanbd.telecom.analysis.mr.kv.CombineDimension;
import net.hanbd.telecom.analysis.mr.kv.DateDimension;
import net.hanbd.telecom.analysis.mr.kv.UserDimension;
import net.hanbd.telecom.analysis.mr.mapper.SummationMapper;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class SummationMrTest {
   /* ImmutableBytesWritable k1;
    Result v1;
    CombineDimension k2;
    IntWritable v2;*/


    private CombineDimension k2Assembly(CombineDimension k2, UserDimension user, DateDimension date) {
        k2.setDateDimension(date);
        k2.setUserDimension(user);
        return k2;
    }

    @Test
    public void testMapper() throws IOException {
        SummationMapper mapper = new SummationMapper();
        MapDriver<ImmutableBytesWritable, Result, CombineDimension, IntWritable> driver = new MapDriver<>(mapper);

        // k1
        String k1Str = "03_18724098345_17395478394_2020-01-03 09:00:24_1323_1";
        ImmutableBytesWritable k1 = new ImmutableBytesWritable();
        k1.set(Bytes.toBytes(k1Str));
        // v1
//        ArrayList<KeyValue> kvs = Lists.newArrayList();
//        kvs.add(new KeyValue(Bytes.toBytes("telecom")));
        Result v1 = new Result();

        // expect k2
        CombineDimension k2 = new CombineDimension();

        UserDimension callerDimension = new UserDimension("18724098345", "王明");
        UserDimension calleeDimension = new UserDimension("17395478394", "欧阳克里");
        DateDimension yearDimension = new DateDimension(2020, -1, -1);
        DateDimension monthDimension = new DateDimension(2020, 1, -1);
        DateDimension dayDimension = new DateDimension(2020, 1, 3);

        // expect v2
        IntWritable v2 = new IntWritable();
        v2.set(1323);

        // run mapper test
        driver.withInput(k1, v1)
                .withOutput(k2Assembly(k2, callerDimension, yearDimension), v2)
                .withOutput(k2Assembly(k2, callerDimension, monthDimension), v2)
                .withOutput(k2Assembly(k2, callerDimension, dayDimension), v2)
                .withOutput(k2Assembly(k2, calleeDimension, yearDimension), v2)
                .withOutput(k2Assembly(k2, calleeDimension, monthDimension), v2)
                .withOutput(k2Assembly(k2, calleeDimension, dayDimension), v2)
                .runTest();
    }


}
