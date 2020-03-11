import org.junit.Test;
import net.hanbd.telecom.producer.CallLog;
import net.hanbd.telecom.producer.CallLogProducer;
import net.hanbd.telecom.producer.User;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CallLogProducerTest {
    CallLogProducer producer = new CallLogProducer(genUsers(), "2020-01-01T00:00:00", "2020-01-30T00:00:00");

    public List<User> genUsers() {
        User u1 = new User(1, "123456789", "ZhangSan");
        User u2 = new User(2, "987654321", "LiSi");
        User u3 = new User(3, "000000000", "WangWu");
        User u4 = new User(4, "111111111", "YanSan");
        List<User> users = new ArrayList<>(2);
        users.add(u1);
        users.add(u2);
        users.add(u3);
        users.add(u4);

        return users;
    }

    @Test
    public void genCallLog() {
        List<CallLog> logs = producer.produce(5);
        logs.forEach(System.out::println);
    }

    @Test
    public void writeLogToFile() {
        producer.produceAndWrite(5, new File("E:/tmp/callLog.txt"));
    }


}
