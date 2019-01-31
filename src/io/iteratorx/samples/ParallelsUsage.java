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

		// by Threads
		{
			// process each item parallelly using thread pool
			Threads.from(jdbcReader.read("select * from tablename")).forEach(item -> {
				System.err.println(item);
			});

			// process batch data parallelly
			Threads.from(jdbcReader.read("select * from tablename")).forBatch(items -> {
				for (final JSONObject item : items) {
					System.err.println(item);
				}
			});
		}

		// by Flink
		{
			// process each item parallelly using Flink engine
			Flink.from(jdbcReader.read("select * from tablename")).forEach(item -> {
				System.err.println(item);
			});

			// process batch data parallelly
			Flink.from(jdbcReader.read("select * from tablename")).forBatch(items -> {
				for (final JSONObject item : items) {
					System.err.println(item);
				}
			});

			// use DataSet directly to enable all Flink power
			Flink.from(jdbcReader.read("select * from tablename")).dataSet().distinct().count();
		}

		// by RxJava
		{
			// process each item parallely using RxJava engine
			RxJava.from(jdbcReader.read("select * from tablename")).forEach(item -> {
				System.err.println(item);
			});

			// process batch data parallely
			RxJava.from(jdbcReader.read("select * from tablename")).forBatch(items -> {
				for (final JSONObject item : items) {
					System.err.println(item);
				}
			});

			// use Observable directly
			RxJava.from(jdbcReader.read("select * from tablename")).observable().distinct().count();
		}
	}
}
