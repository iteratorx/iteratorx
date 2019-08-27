package io.iteratorx.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.Assert;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;

public class JdbcWriter {
	private static final Logger logger = Logger.getLogger(JdbcWriter.class.getName());

	private final DataSource dataSource;

	protected int batchSize = 1000;
	protected int queryTimeout = 60;

	private Connection conn = null;
	private PreparedStatement ps = null;
	private final ResultSet rs = null;

	public JdbcWriter(final DataSource dataSource) {
		Assert.assertNotNull(dataSource);
		this.dataSource = dataSource;
	}

	public JdbcWriter setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * set timeout for insert/update
	 */
	public JdbcWriter setQueryTimeout(final int seconds) {
		this.queryTimeout = seconds;
		return this;
	}

	public int execute(final String sql, final Object... parameters) {
		try {

			conn = dataSource.getConnection();
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < parameters.length; i++) {
				ps.setObject(i + 1, parameters[i]);
			}
			ps.setQueryTimeout(queryTimeout);

			return ps.executeUpdate();

		} catch (final SQLException e) {
			throw new RuntimeException(e);

		} finally {
			IOUtils.close(rs, ps, conn);
		}
	}

	public int[] executeBatch(final String sql, final Object[]... records) {
		Preconditions.checkArgument(records.length <= this.batchSize,
				"records size is larger than max batch size, invoke executeBatch(final String sql, final Iterable<Object[]> records) instead of this one");

		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(sql);
			for (final Object[] record : records) {
				for (int i = 0; i < record.length; i++) {
					ps.setObject(i + 1, record[i]);
				}
				ps.addBatch();
			}
			ps.setQueryTimeout(queryTimeout);

			return ps.executeBatch();

		} catch (final SQLException e) {
			throw new RuntimeException(e);

		} finally {
			IOUtils.close(rs, ps, conn);
		}
	}

	public int[][] executeBatch(final String sql, final Iterable<Object[]> records) {
		final Collection<int[]> results = new ArrayList<>();

		final Collection<Object[]> group = new ArrayList<>();
		final Iterator<Object[]> iterator = records.iterator();
		while (iterator.hasNext()) {
			group.add(iterator.next());

			if (group.size() == batchSize) {
				executeBatch(sql, group.toArray(new Object[0][]));
				group.clear();
			}
		}
		if (group.size() > 0) {
			results.add(executeBatch(sql, group.toArray(new Object[0][])));
		}

		return results.toArray(new int[0][]);
	}

	/**
	 * execute batch, each record combined from JSONObject by mapping key.
	 * 
	 * e.g. writeBatch("insert into tablename values(?, ?)", records, "name",
	 * "type");
	 * 
	 * @param sql
	 *            sql to execute
	 * @param records
	 *            each JSONOjbect contains all values for one record
	 * @param mappingKeys
	 *            select values by key from each JSONObject
	 */
	public int[][] executeBatch(final String sql, final Iterable<JSONObject> records, final String... mappingKeys) {
		return executeBatch(sql, new Iterable<Object[]>() {

			@Override
			public Iterator<Object[]> iterator() {
				final Iterator<JSONObject> iterator = records.iterator();
				return new Iterator<Object[]>() {

					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Object[] next() {
						final JSONObject record = iterator.next();
						final String[] keys = mappingKeys.length != 0 ? mappingKeys
								: record.keySet().toArray(new String[0]);

						final Collection<Object> next = new ArrayList<>();
						for (final String value : keys) {
							next.add(record.get(value));
						}
						return next.toArray();
					}
				};
			}
		});

	}

	public JdbcReader asJdbcReader() {
		return new JdbcReader(this.dataSource);
	}
}
