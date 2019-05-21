package io.iteratorx.reader;

import io.iteratorx.reader.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IOUtilsTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetDriverFromDriver() {
    Assert.assertEquals("foo", IOUtils.getDriver("foo", "foo"));
  }

  @Test
  public void testGetDriverFromURL() {
    Assert.assertEquals("com.mysql.jdbc.Driver",
            IOUtils.getDriver("JdBC:MYsql", null));
    Assert.assertEquals("oracle.jdbc.OracleDriver",
            IOUtils.getDriver("JdBC:ORAcleb", null));
    Assert.assertEquals("com.ibm.db2.jcc.DB2Driver",
            IOUtils.getDriver("jDbC:DB2\u1040", null));
    Assert.assertEquals("org.apache.derby.jdbc.EmbeddedDriver",
            IOUtils.getDriver("JdBC:DERby", null));
    Assert.assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver",
            IOUtils.getDriver("JdBC:SQLserVER", null));
    Assert.assertEquals("org.apache.hive.jdbc.HiveDriver",
            IOUtils.getDriver("jdBC:HIve2", null));
    Assert.assertEquals("org.sqlite.JDBC",
            IOUtils.getDriver("JdBC:SQLiteEs", null));
    Assert.assertEquals("org.postgresql.Driver",
            IOUtils.getDriver("JdBC:POStGrESQL\u0871", null));
    Assert.assertEquals("org.apache.ignite.IgniteJdbcThinDriver",
            IOUtils.getDriver("JdBC:IGNiTeESQ", null));
  }

  @Test
  public void testGetDriverInvalidURL() {
    thrown.expect(IllegalStateException.class);
    IOUtils.getDriver("", null);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void testGetDriverNoURLOrDriver() {
    thrown.expect(NullPointerException.class);
    IOUtils.getDriver(null, null);

    // Method is not expected to return due to exception thrown
  }
}
