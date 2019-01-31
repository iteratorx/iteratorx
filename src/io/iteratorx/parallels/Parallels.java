package io.iteratorx.parallels;

public interface Parallels {

	public static int DefaultParallelism = 3 * Runtime.getRuntime().availableProcessors();

	public static int DefaultBatchSize = 1000;

}
