package io.iteratorx.reader;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

public abstract class DataSourceBuilder {

	public static class JdbcDataSourceBuilder extends DataSourceBuilder {

		@Override
		public DataSource build() {
			return new DataSource() {

				@Override
				public <T> T unwrap(final Class<T> iface) throws SQLException {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean isWrapperFor(final Class<?> iface) throws SQLException {
					throw new UnsupportedOperationException();
				}

				@Override
				public Logger getParentLogger() throws SQLFeatureNotSupportedException {
					throw new UnsupportedOperationException();
				}

				@Override
				public void setLoginTimeout(final int seconds) throws SQLException {
					DriverManager.setLoginTimeout(seconds);
				}

				@Override
				public void setLogWriter(final PrintWriter out) throws SQLException {
					DriverManager.setLogWriter(out);
				}

				@Override
				public int getLoginTimeout() throws SQLException {
					return DriverManager.getLoginTimeout();
				}

				@Override
				public PrintWriter getLogWriter() throws SQLException {
					return DriverManager.getLogWriter();
				}

				@Override
				public Connection getConnection() throws SQLException {
					return getConnection(properties.getProperty("user"), properties.getProperty("password"));
				}

				@Override
				public Connection getConnection(final String user, final String password) throws SQLException {

					properties.setProperty("user", user);
					properties.setProperty("password", password);

					try {
						final String url = properties.getProperty("url");

						final String driver = IOUtils.getDriver(url, properties.getProperty("driver"));
						Class.forName(driver);

						final int loginTimeout = properties.get("loginTimeout") == null ? 60
								: (int) properties.get("loginTimeout");
						DriverManager.setLoginTimeout(loginTimeout);

						final Connection conn = DriverManager.getConnection(url, properties);
						if (conn.isClosed()) {
							throw new SQLException("connection closed!");
						}
						return conn;

					} catch (final ClassNotFoundException e) {
						throw new RuntimeException(e);
					}
				}

			};
		}
	}

	protected final Properties properties = new Properties();

	protected DataSourceBuilder() {
	}

	public DataSourceBuilder setDriver(final String driver) {
		return set("driver", driver);
	}

	public DataSourceBuilder setUrl(final String url) {
		return set("url", url);
	}

	public DataSourceBuilder setUser(final String user) {
		return set("user", user);
	}

	public DataSourceBuilder setPassword(final String password) {
		return set("password", password);
	}

	public DataSourceBuilder setLoginTimeout(final int seconds) {
		return set("loginTimeout", seconds);
	}

	public DataSourceBuilder set(final String key, final Object value) {
		properties.put(key, value);
		return this;
	}

	public abstract DataSource build();

}
