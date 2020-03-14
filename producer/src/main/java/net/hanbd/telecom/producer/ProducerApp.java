package net.hanbd.telecom.producer;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 生产通信纪录测试数据。
 *
 * @author hanbd
 */
public class ProducerApp {

    private static List<User> getUsersFormDb() {
        final String JDBC_URL = "jdbc:mysql://centos:3306/telecom?useSSL=false";
        final String USER = "telecom";
        final String PASSWORD = "telecom";

        List<User> users = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            PreparedStatement ps = conn.prepareStatement("select * from user");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    users.add(new User(rs.getInt("id"), rs.getString("phone"),
                            rs.getString("name")));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return users;
    }


    public static void main(String[] args) {
        int num = Integer.parseInt(args[0]);
        String filePath = args[1];
        String from = args[2];
        String to = args[3];

        System.out.println(num);
        System.out.println(filePath);
        System.out.println(from);
        System.out.println(to);

        CallLogProducer producer = new CallLogProducer(ProducerApp.getUsersFormDb(), from, to);


        boolean b = producer.produceAndWrite(num, new File(filePath));
        if (b) {
            System.out.println("SUCCESS: generate " + num + " logs to " + filePath);
        }
    }

}
