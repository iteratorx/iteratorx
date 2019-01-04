package io.iteratorx.reader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import com.google.common.base.Throwables;

public class DbUtils {
	private static final Logger logger = Logger.getLogger(DbUtils.class.getName());

	public static String getDriver(String url, final String driver) {
		if (driver != null) {
			return driver;
		}

		url = url.toLowerCase();
		if (url.startsWith("jdbc:postgresql")) {
			return "org.postgresql.Driver";
		} else if (url.startsWith("jdbc:mysql")) {
			return "com.mysql.jdbc.Driver";
		} else if (url.startsWith("jdbc:oracle")) {
			return "oracle.jdbc.OracleDriver";
		} else if (url.startsWith("jdbc:sqlserver")) {
			return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		} else if (url.startsWith("jdbc:db2")) {
			return "com.ibm.db2.jcc.DB2Driver";
		} else if (url.startsWith("jdbc:ignite")) {
			return "org.apache.ignite.IgniteJdbcThinDriver";
		} else if (url.startsWith("jdbc:derby")) {
			return "org.apache.derby.jdbc.EmbeddedDriver";
		} else {
			return null;
		}
	}

	public static void close(final ResultSet rs, final Statement stmt, final Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (final SQLException e) {
				logger.warning(Throwables.getStackTraceAsString(e));
			}
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (final SQLException e) {
				logger.warning(Throwables.getStackTraceAsString(e));
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (final SQLException e) {
				logger.warning(Throwables.getStackTraceAsString(e));
			}
		}
	}
}
