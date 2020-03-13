import net.hanbd.telecom.analysis.model.User;
import net.hanbd.telecom.analysis.mr.kv.DateDimension;
import net.hanbd.telecom.analysis.mr.kv.UserDimension;
import net.hanbd.telecom.analysis.util.TelecomDao;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class MysqlTest {
    private TelecomDao dao = new TelecomDao();

    @Test
    public void telecomDaoTest() throws SQLException {
        User u1 = dao.getUserByPhone("17450928450").get();
        User u2 = dao.getUserByPhone("18327344958").get();
        User u3 = dao.getUserByPhone("17284937849").get();
        User u4 = dao.getUserByPhone("12345").orElse(new User());

        Assert.assertEquals("王富贵", u1.getName());
        Assert.assertEquals("韩牧", u2.getName());
        Assert.assertEquals("慕容飞", u3.getName());
        Assert.assertNull(u4.getName());
    }

    @Test
    public void getDimensionIdTest() {
        DateDimension date = new DateDimension(2020, -1, -1);
        DateDimension date2 = new DateDimension(2020, 12, 16);
        UserDimension user = new UserDimension("13967867800", "李雷");

        int dateId = dao.getDimensionId(date2).get();
//        Assert.assertEquals(dateId, 1);

        int userId = dao.getDimensionId(user).get();
        Assert.assertEquals(userId, 1);
    }
}
