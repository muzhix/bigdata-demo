package net.hanbd.telecom.consumer;

import net.hanbd.telecom.consumer.kafka.HBaseConsumer;

/**
 * @author hanbd
 */
public class ConsumerApp {
    public static void main(String[] args) {
        HBaseConsumer consumer = new HBaseConsumer();
        consumer.consume();
    }
}
