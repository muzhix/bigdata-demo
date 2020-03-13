package net.hanbd.telecom.analysis.mr.output;

import lombok.SneakyThrows;
import net.hanbd.telecom.analysis.mr.kv.CombineDimension;
import net.hanbd.telecom.analysis.mr.kv.Summation;
import net.hanbd.telecom.analysis.util.TelecomDao;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hanbd
 */
public class MysqlOutputFormat extends OutputFormat<CombineDimension, Summation> {
    private OutputCommitter committer;
    private TelecomDao dao = new TelecomDao();

    @Override
    public RecordWriter<CombineDimension, Summation> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        return new TelecomRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        if (committer == null) {
            String name = taskAttemptContext.getConfiguration().get(FileOutputFormat.OUTDIR);
            Path outputPath = name == null ? null : new Path(name);
            committer = new FileOutputCommitter(outputPath, taskAttemptContext);
        }
        return committer;
    }

    private static class TelecomRecordWriter extends RecordWriter<CombineDimension, Summation> {
        private static final int MAX_BATCH = 63;

        private TelecomDao dao = new TelecomDao();
        private List<net.hanbd.telecom.analysis.model.Summation> summationPojoList = new ArrayList<>(64);

        public TelecomRecordWriter() {
            super();
        }

        @SneakyThrows(SQLException.class)
        @Override
        public void write(CombineDimension combine, Summation summation) throws IOException,
                InterruptedException {
            int dateDimenId = dao.getDimensionId(combine.getDateDimension()).orElse(0);
            int userDimenId = dao.getDimensionId(combine.getUserDimension()).orElse(0);

            summationPojoList.add(genSummationPojo(dateDimenId, userDimenId, summation));
            if (summationPojoList.size() >= MAX_BATCH) {
                // batch insert
                dao.insertSummationBatch(summationPojoList);
            }
        }

        @SneakyThrows(SQLException.class)
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (!summationPojoList.isEmpty()) {
                dao.insertSummationBatch(summationPojoList);
            }
        }

        private net.hanbd.telecom.analysis.model.Summation genSummationPojo(int idDate, int idUser,
                                                                            Summation summation) {
            return new net.hanbd.telecom.analysis.model.Summation(idDate, idUser, summation.getCallSum(),
                    summation.getCallDurationSum());

        }
    }


}
