package io.iteratorx.samples;

import java.util.Collection;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.reader.DataSourceBuilder.JdbcDataSourceBuilder;
import io.iteratorx.reader.JdbcReader;

public class JdbcReaderUsage {

	public static void main(final String[] args) throws Exception {

		// create jdbc reader
		final JdbcReader jdbcReader = new JdbcReader(
				new JdbcDataSourceBuilder().setUrl("jdbc:postgresql://10.23.112.2:3333/dbname").setUser("username")
						.setPassword("password").build());

		// fetch by iterable
		for (final JSONObject item : jdbcReader.read("select * from tablename")) {
			System.err.println(item);
		}

		// fetch all into one collection
		final Collection<JSONObject> items = jdbcReader.readAll("select * from tablename where id = ?", "param");
		for (final JSONObject item : items) {
			System.err.println(item);
		}

	}
}
