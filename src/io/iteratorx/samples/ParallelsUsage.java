package io.iteratorx.samples;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.parallels.Flink;
import io.iteratorx.parallels.RxJava;
import io.iteratorx.parallels.Threads;
import io.iteratorx.reader.DataSourceBuilder.JdbcDataSourceBuilder;
import io.iteratorx.reader.JdbcReader;

public class ParallelsUsage {

	public static void main(final String[] args) throws Exception {

		// create jdbc reader
		final JdbcReader jdbcReader = new JdbcReader(
				new JdbcDataSourceBuilder().setUrl("jdbc:postgresql://10.23.112.2:3333/dbname").setUser("username")
						.setPassword("password").build());

		// by multi-threads
		Threads.create(jdbcReader.read("select * from tablename")).forEach(item -> {
			System.err.println(item);
		});
		Threads.create(jdbcReader.read("select * from tablename")).forBatch(items -> {
			for (final JSONObject item : items) {
				System.err.println(item);
			}
		});

		// by RxJava
		RxJava.create(jdbcReader.read("select * from tablename")).forEach(item -> {
			System.err.println(item);
		});
		RxJava.create(jdbcReader.read("select * from tablename")).forBatch(items -> {
			for (final JSONObject item : items) {
				System.err.println(item);
			}
		});

		// by Flink
		Flink.create(jdbcReader.read("select * from tablename")).forEach(item -> {
			System.err.println(item);
		});
		Flink.create(jdbcReader.read("select * from tablename")).forBatch(items -> {
			for (final JSONObject item : items) {
				System.err.println(item);
			}
		});
	}
}
