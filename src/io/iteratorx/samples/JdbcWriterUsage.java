package io.iteratorx.samples;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.reader.DataSourceBuilder.JdbcDataSourceBuilder;
import io.iteratorx.reader.JdbcWriter;

public class JdbcWriterUsage {

	public static void main(final String[] args) throws Exception {

		// create jdbc Writer
		final JdbcWriter jdbcWriter = new JdbcWriter(
				new JdbcDataSourceBuilder().setUrl("jdbc:postgresql://10.23.112.2:3333/dbname").setUser("username")
						.setPassword("password").build());

		// write
		jdbcWriter.execute("insert into tablename values(?, ?)", "name", "type");

		// write batch by iterable
		final Iterable<JSONObject> iter = jdbcWriter.asJdbcReader().read("select name, type from tablename");
		jdbcWriter.executeBatch("insert into tablename values(?, ?)", iter);

	}
}
