package io.iteratorx.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.Assert;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.AutoCloseableIterator;

public class JdbcReader {
	private static final Logger logger = Logger.getLogger(JdbcReader.class.getName());

	private final DataSource dataSource;

	protected int batchSize = 1000;
	protected int queryTimeout = 60 * 60;

	private JSONObject columnMetaData = null;

	private Connection conn = null;
	private PreparedStatement ps = null;
	private ResultSet rs = null;

	public JdbcReader(final DataSource dataSource) {
		Assert.assertNotNull(dataSource);
		this.dataSource = dataSource;
	}

	public JdbcReader setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public JdbcReader setQueryTimeout(final int seconds) {
		this.queryTimeout = seconds;
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

	/**
	 * Read each jdbc Table Row into JSONObject iteratively.
	 * 
	 * NOTICE: thread-unsafe: do not use result Iterable in multi-threads!
	 * 
	 */
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
			IOUtils.close(rs, ps, conn);

			throw new RuntimeException(e);
		}

		// return iterator
		return new Iterable<JSONObject>() {

			@Override
			public Iterator<JSONObject> iterator() {
				return new AutoCloseableIterator<JSONObject>() {
					private static final long serialVersionUID = -7178131452993168194L;

					boolean hasNext = true;

					@Override
					public boolean hasNext() {
						if (hasNext == false) {
							// already closed

							return false;
						}

						try {
							hasNext = rs.next();
							if (hasNext == false) {
								close();
							}

							return hasNext;

						} catch (final SQLException e) {
							close();

							throw new RuntimeException(e);
						}
					}

					@Override
					public JSONObject next() {
						try {
							final JSONObject data = new JSONObject(true);
							for (final String column : getColumnMetaData().keySet()) {
								data.put(column, rs.getObject(column));
							}

							return data;

						} catch (final SQLException e) {
							close();

							throw new RuntimeException(e);
						}
					}

					@Override
					public void close() {
						IOUtils.close(rs, ps, conn);
						hasNext = false;
					}
				};

			}
		};

	}

	protected JSONObject parseColumnMetaData(final ResultSetMetaData resultSetMetaData) throws SQLException {
		final JSONObject metadata = new JSONObject(true);

		for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
			final JSONObject columnMetaData = new JSONObject();
			metadata.put(resultSetMetaData.getColumnName(i), columnMetaData);

			columnMetaData.put("ColumnType", resultSetMetaData.getColumnType(i));
			columnMetaData.put("ColumnTypeName", resultSetMetaData.getColumnTypeName(i));
			columnMetaData.put("ColumnClassName", resultSetMetaData.getColumnClassName(i));
		}
		return metadata;
	}

	public JdbcWriter asJdbcWriter() {
		return new JdbcWriter(this.dataSource);
	}
}
