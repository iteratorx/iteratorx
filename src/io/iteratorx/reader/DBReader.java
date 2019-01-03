package io.iteratorx.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.Assert;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;

public class DBReader {
	private static final Logger logger = Logger.getLogger(DBReader.class.getName());

	private final DataSource dataSource;

	protected int batchSize = 1000;
	protected int queryTimeout = 60 * 60;

	protected JSONObject columnMetaData = null;

	private Connection conn = null;
	private PreparedStatement ps = null;
	private ResultSet rs = null;

	public DBReader(final DataSource dataSource) {
		Assert.assertNotNull(dataSource);
		this.dataSource = dataSource;
	}

	public DBReader setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public DBReader setQueryTimeout(final int queryTimeout) {
		this.queryTimeout = queryTimeout;
		return this;
	}

	public JSONObject getColumnMetaData() {
		return this.columnMetaData;
	}

	public List<JSONObject> readAll(final String sql, final Object... parameters) {
		final List<JSONObject> results = new ArrayList<>();
		for (final JSONObject one : read(sql, parameters)) {
			results.add(one);
		}
		return results;
	}

	public Iterable<JSONObject> read(final String sql, final Object... parameters) {

		// query batch
		try {
			// query
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			for (int i = 0; i < parameters.length; i++) {
				ps.setObject(i + 1, parameters[i]);
			}
			ps.setFetchSize(batchSize);
			ps.setQueryTimeout(queryTimeout);
			rs = ps.executeQuery();

			// get column metadata
			columnMetaData = parseColumnMetaData(rs.getMetaData());

		} catch (final SQLException e) {
			close(rs, ps, conn);

			throw new RuntimeException(e);
		}

		// return iterator
		return new Iterable<JSONObject>() {

			@Override
			public Iterator<JSONObject> iterator() {
				return new Iterator<JSONObject>() {

					@Override
					public boolean hasNext() {
						try {
							final boolean hasNext = rs.next();
							if (hasNext == false) {
								close(rs, ps, conn);
							}

							return hasNext;

						} catch (final SQLException e) {
							close(rs, ps, conn);

							throw new RuntimeException(e);
						}
					}

					@Override
					public JSONObject next() {
						try {
							final JSONObject data = new JSONObject();
							for (final String column : getColumnMetaData().keySet()) {
								data.put(column, rs.getObject(column));
							}

							return data;

						} catch (final SQLException e) {
							close(rs, ps, conn);

							throw new RuntimeException(e);
						}
					}

				};
			}
		};

	}

	protected void close(final ResultSet rs, final Statement stmt, final Connection conn) {
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

	protected JSONObject parseColumnMetaData(final ResultSetMetaData resultSetMetaData) throws SQLException {
		final JSONObject metadata = new JSONObject();

		for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
			final JSONObject columnMetaData = new JSONObject();
			metadata.put(resultSetMetaData.getColumnName(i), columnMetaData);

			columnMetaData.put("ColumnType", resultSetMetaData.getColumnType(i));
			columnMetaData.put("ColumnTypeName", resultSetMetaData.getColumnTypeName(i));
			columnMetaData.put("ColumnClassName", resultSetMetaData.getColumnClassName(i));
		}
		return metadata;
	}

}
