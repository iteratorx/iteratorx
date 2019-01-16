# IteratorX: simplest iterator for IO

## 	
	// create reader
	final JdbcReader jdbcReader = new JdbcReader(
			new JdbcDataSourceBuilder().setUrl("jdbc:postgresql://ip:port/dbname").setUser("username")
					.setPassword("password").build());
	
	// by iterator
	for (final JSONObject item : jdbcReader.read("select * from tablename")) {
		System.err.println(item);
	}
	
	// fetch all
	for (final JSONObject item : jdbcReader.readAll("select * from tablename")) {
		System.err.println(item);
	}
