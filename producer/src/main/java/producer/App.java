package producer;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hanbd
 */
public class App {

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
        if (args.length < 2) {
            System.out.println("usage: [num] [filePath]");
            return;
        }

        CallLogProducer producer = new CallLogProducer(App.getUsersFormDb(), "2020-01-01T00:00:00",
                "2020-01-30T00:00:00");

        int num = Integer.parseInt(args[0]);
        String filePath = args[1];

        boolean b = producer.produceAndWrite(num, new File(filePath));
        if (b) {
            System.out.println("SUCCESS: generate " + num + " logs to " + filePath);
        }
    }

}
