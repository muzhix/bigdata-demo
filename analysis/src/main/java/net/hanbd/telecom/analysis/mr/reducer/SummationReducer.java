package net.hanbd.telecom.analysis.mr.reducer;

import net.hanbd.telecom.analysis.mr.kv.CombineDimension;
import net.hanbd.telecom.analysis.mr.kv.Summation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hanbd
 */
public class SummationReducer extends Reducer<CombineDimension, IntWritable, CombineDimension, Summation> {
    private Summation summation = new Summation();

    @Override
    protected void reduce(CombineDimension key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        int callSum = 0;
        int callDurationSum = 0;

        for (IntWritable duration : values) {
            callSum++;
            callDurationSum += duration.get();
        }
        summation.setCallSum(callSum);
        summation.setCallDurationSum(callDurationSum);

        context.write(key, summation);
    }
}
