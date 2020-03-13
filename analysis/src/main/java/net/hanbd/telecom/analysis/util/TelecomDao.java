package net.hanbd.telecom.analysis.util;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import net.hanbd.telecom.analysis.model.Date;
import net.hanbd.telecom.analysis.model.Summation;
import net.hanbd.telecom.analysis.model.User;
import net.hanbd.telecom.analysis.mr.kv.DateDimension;
import net.hanbd.telecom.analysis.mr.kv.BaseDimension;
import net.hanbd.telecom.analysis.mr.kv.UserDimension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * @author hanbd
 */
@NoArgsConstructor
@Log4j
public class TelecomDao {

    /**
     * 根据电话号返回姓名
     *
     * @param phone 电话号
     * @return 用户姓名
     */
    @SneakyThrows(SQLException.class)
    public Optional<User> getUserByPhone(String phone) {
        @Cleanup Connection conn = JdbcUtil.getConnection();
        PreparedStatement ps = conn.prepareStatement("select id,phone,name from user where phone = ?");
        ps.setString(1, phone);
        @Cleanup ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            User u = new User();
            u.setId(rs.getInt("id"));
            u.setPhone(rs.getString("phone"));
            u.setName(rs.getString("name"));
            return Optional.of(u);
        }

        return Optional.empty();
    }

    @SneakyThrows(SQLException.class)
    public Optional<Date> getDate(int year, int month, int day) {
        @Cleanup Connection conn = JdbcUtil.getConnection();
        PreparedStatement ps = conn.prepareStatement("select id,year,month,day from date where year = ? and month = ?" +
                " and day = ?");
        ps.setInt(1, year);
        ps.setInt(2, month);
        ps.setInt(3, day);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            Date date = new Date(
                    rs.getInt("id"),
                    rs.getInt("year"),
                    rs.getInt("month"),
                    rs.getInt("day")
            );
            System.out.println("=====================================" + date.toString());
            return Optional.of(date);
        }
        return Optional.empty();
    }

    public Optional<Date> getDate(DateDimension dimen) {
        return getDate(dimen.getYear(), dimen.getMonth(), dimen.getDay());
    }

    /**
     * 批量插入最终结果 Summation
     *
     * @param summationPojoList summation list
     * @throws SQLException insert exception
     */
    public void insertSummationBatch(List<Summation> summationPojoList) throws SQLException {
        @Cleanup Connection conn = JdbcUtil.getConnection();
        // 关闭自动提交
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("insert into summation (id_date_user, id_date, id_user, " +
                "call_sum, call_duration_sum) values (?,?,?,?,?) on duplicate key update id_date_user = ?, call_sum =" +
                " ?, call_duration_sum = ?");

        for (Summation o : summationPojoList) {
            // 无则插入
            ps.setString(1, o.getIdDateUser());
            ps.setInt(2, o.getIdDate());
            ps.setInt(3, o.getIdUser());
            ps.setInt(4, o.getCallSum());
            ps.setInt(5, o.getCallDurationSum());
            // 有则更新
            ps.setString(6, o.getIdDateUser());
            ps.setInt(7, o.getCallSum());
            ps.setInt(8, o.getCallDurationSum());
            ps.addBatch();
        }

        // execute
        ps.executeBatch();
        conn.commit();
        ps.clearBatch();
    }

    private int insertDate(Date date) throws SQLException {
        @Cleanup Connection conn = JdbcUtil.getConnection();
        PreparedStatement ps = conn.prepareStatement("insert into date (year, month, day) values (?,?,?)");
        ps.setInt(1, date.getYear());
        ps.setInt(2, date.getMonth());
        ps.setInt(3, date.getDay());
        return ps.executeUpdate();
    }

    /**
     * 插入DateDimension
     *
     * @param dimen DateDimension
     * @return success or not
     */
    private int insertDateDimension(DateDimension dimen) throws SQLException {
        Date date = new Date();
        date.setYear(dimen.getYear());
        date.setMonth(dimen.getMonth());
        date.setDay(dimen.getDay());
        return insertDate(date);
    }

    /**
     * 根据数据维度获取id
     * TODO 在此处加入redis缓存，更快读取
     *
     * @param dimen Dimension
     * @return Optional<Integer> id
     */
    public Optional<Integer> getDimensionId(BaseDimension dimen) {
        if (dimen instanceof UserDimension) {
            return getUserDimensionId((UserDimension) dimen);
        } else if (dimen instanceof DateDimension) {
            return getDateDimensionId((DateDimension) dimen);
        } else {
            return Optional.empty();
        }
    }

    public Optional<Integer> getUserDimensionId(UserDimension dimen) {
        Optional<User> optional = getUserByPhone(dimen.getPhone());
        return optional.isPresent() ? optional.map(User::getId) : Optional.empty();
    }

    @SneakyThrows(SQLException.class)
    public Optional<Integer> getDateDimensionId(DateDimension dimen) {
        Optional<Date> optional = getDate(dimen);
        if (optional.isPresent()) {
            // 库中已存在，直接返回
            return optional.map(Date::getId);
        } else {
            // 库中不存在，先插入再查
            int b = insertDateDimension(dimen);
            if (b > 0) {
                optional = getDate(dimen);
                return optional.isPresent() ? optional.map(Date::getId) : Optional.empty();
            } else {
                throw new SQLException("insert DateDimension error");
            }
        }
    }
}
