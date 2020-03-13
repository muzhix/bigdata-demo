import net.hanbd.telecom.analysis.util.PropertyUtil;
import org.junit.Assert;
import org.junit.Test;

public class PropertyUtilTest {
    @Test
    public void test() {
        String tableName = PropertyUtil.getProperty("hbase.tableName");
        String user = PropertyUtil.getProperty("mysql.user");

        Assert.assertEquals("telecom.calllog", tableName);
        Assert.assertEquals("telecom", user);
    }
}
