import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class mrTest {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Splitter SPLITTER = Splitter.on("_");
    private static final String CALLER_FLAG = "1";

    @Test
    public void test() {
        String rowKey = "05_17450928450_12839341289_2020-01-23 00:44:58_948_1";
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

        int year = time.getYear();
        int month = time.getMonthValue();
        int day = time.getDayOfMonth();
        System.out.println(year);
        System.out.println(month);
        System.out.println(day);
    }
}
