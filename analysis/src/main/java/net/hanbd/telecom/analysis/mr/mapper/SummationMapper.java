package net.hanbd.telecom.analysis.mr.mapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import net.hanbd.telecom.analysis.model.User;
import net.hanbd.telecom.analysis.mr.kv.CombineDimension;
import net.hanbd.telecom.analysis.mr.kv.DateDimension;
import net.hanbd.telecom.analysis.mr.kv.UserDimension;
import net.hanbd.telecom.analysis.util.TelecomDao;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @author hanbd
 */
@Log4j
public class SummationMapper extends TableMapper<CombineDimension, IntWritable> {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Splitter SPLITTER = Splitter.on("_");
    /**
     * 主叫标志
     */
    private static final String CALLER_FLAG = "1";
    /**
     * 维度占位符。
     * 时间维度 (2014, -1 , -1)表示聚合2014年所有通话信息
     * 时间维度 (2014, 10 , -1)表示聚合2014年10月所有通话信息
     * 时间维度 (2014, 10 , 12)表示聚合2014年10月12日所有通话信息
     */
    private static final int PLACEHOLDER = -1;

    private TelecomDao dao = new TelecomDao();
    private CombineDimension combine = new CombineDimension();
    private IntWritable durationInt = new IntWritable();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {

        // rowKey format: 03_18724098345_17395478394_2020-01-03 09:00:24_1323_1
        //                regionCode_caller_callee_establishTime_duration_flag
        String rowKey = Bytes.toString(key.get());
        List<String> keys = Lists.newArrayList(SPLITTER.split(rowKey));

        if (!CALLER_FLAG.equals(keys.get(5))) {
            // 只聚合主叫数据
            return;
        }
        String caller = keys.get(1);
        String callee = keys.get(2);
        String timeStr = keys.get(3);
        String duration = keys.get(4);
        LocalDateTime time = LocalDateTime.parse(timeStr, FORMATTER);
        durationInt.set(Integer.parseInt(duration));

        int year = time.getYear();
        int month = time.getMonthValue();
        int day = time.getDayOfMonth();

        // 定义用户维度
        UserDimension callerDimen = new UserDimension(caller, getName(caller));
        UserDimension calleeDimen = new UserDimension(caller, getName(callee));

        // 聚合
        contextWrite(context, callerDimen, year, month, day);
        contextWrite(context, calleeDimen, year, month, day);
    }

    /**
     * 根据不同的用户维度与时间维度，聚合写入
     *
     * @param context   上下文
     * @param userDimen 用户维度
     * @param year      年
     * @param month     月
     * @param day       日
     */
    @SneakyThrows
    private void contextWrite(Context context, UserDimension userDimen, int year, int month, int day) {
        // 设定用户维度
        combine.setUserDimension(userDimen);

        // 定义不同时间维度
        DateDimension yearDimen = new DateDimension(year, PLACEHOLDER, PLACEHOLDER);
        DateDimension monthDimen = new DateDimension(year, month, PLACEHOLDER);
        DateDimension dayDimen = new DateDimension(year, month, day);

        // 根据不同的时间维度，分别写入. 时间维度 年、月、日
        combine.setDateDimension(yearDimen);
        context.write(combine, durationInt);

        combine.setDateDimension(monthDimen);
        context.write(combine, durationInt);

        combine.setDateDimension(dayDimen);
        context.write(combine, durationInt);
    }

    /**
     * 根据电话号获取姓名
     *
     * @param phone phoneNum
     * @return name
     */
    private String getName(String phone) {
        User u = dao.getUserByPhone(phone).orElse(null);
        return u != null ? u.getName() : "$ERROR$";
    }


}
