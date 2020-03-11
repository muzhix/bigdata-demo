package net.hanbd.telecom.consumer.kafka;

import net.hanbd.telecom.consumer.hbase.CallLogDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import net.hanbd.telecom.consumer.util.PropertyUtil;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author hanbd
 */
public class HBaseConsumer implements Consumer {

    private Properties props = PropertyUtil.getAllProperties();

    @Override
    public void consume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(PropertyUtil.getProperty("kafka.topics")));

        CallLogDao dao = CallLogDao.getInstance();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> cr : records) {
                String value = cr.value();
                System.out.println(value);
                // 写入hbase
                dao.put(value);
            }
        }
    }
}
