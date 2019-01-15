package io.iteratorx.samples;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.reader.DataSourceBuilder.JdbcDataSourceBuilder;
import io.iteratorx.reader.JdbcReader;

public class Usage {

	public static void main(final String[] args) throws Exception {
		final JdbcReader jdbcReader = new JdbcReader(
				new JdbcDataSourceBuilder().setUrl("jdbc:postgresql://10.23.112.2:3333/dbname").setUser("username")
						.setPassword("password").build());

		// by iterator
		for (final JSONObject item : jdbcReader.read("select * from tablename")) {
			System.err.println(item);
		}

		// fetch all
		for (final JSONObject item : jdbcReader.readAll("select * from tablename")) {
			System.err.println(item);
		}
	}
}
