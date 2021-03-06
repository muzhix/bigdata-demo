package net.hanbd.telecom.consumer.util;

import java.io.*;
import java.util.Properties;

/**
 * @author hanbd
 */
public class PropertyUtil {
    private static Properties props;

    static {
        load();
    }

    private synchronized static void load() {
        props = new Properties();
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties")) {
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        if (null == props) {
            load();
        }
        return props.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        if (null == props) {
            load();
        }
        return props.getProperty(key, defaultValue);
    }

    public static Properties getAllProperties() {
        if (null == props) {
            load();
        }
        return props;
    }
}
