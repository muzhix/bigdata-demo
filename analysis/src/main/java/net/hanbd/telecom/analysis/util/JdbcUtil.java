package net.hanbd.telecom.analysis.util;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author hanbd
 */
public class JdbcUtil {
    private static final String TELECOM_URL = PropertyUtil.getProperty("mysql.url");
    private static final String TELECOM_USER = PropertyUtil.getProperty("mysql.user");
    private static final String TELECOM_PASSWORD = PropertyUtil.getProperty("mysql.password");
    private static final int INIT_SIZE = 5;
    private static final int MIN_SIZE = 10;
    private static final int MAX_SIZE = 20;


    private static DruidDataSource dataSource;

    static {
        initDataSource();
    }

    @SneakyThrows(SQLException.class)
    public static Connection getConnection() {
        if (dataSource.isClosed() || !dataSource.isEnable()) {
            initDataSource();
        }

        return dataSource.getConnection();
    }

    private synchronized static void initDataSource() {
        dataSource = new DruidDataSource();

        dataSource.setUrl(TELECOM_URL);
        dataSource.setUsername(TELECOM_USER);
        dataSource.setPassword(TELECOM_PASSWORD);
        // 配置初始大小、最小、最大
        dataSource.setInitialSize(INIT_SIZE);
        dataSource.setMinIdle(MIN_SIZE);
        dataSource.setMaxActive(MAX_SIZE);
    }

}
