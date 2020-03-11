import org.junit.Test;
import net.hanbd.telecom.producer.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DBTest {

    @Test
    public void getUsersFormDb() {
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

        users.forEach(System.out::println);
    }
}
