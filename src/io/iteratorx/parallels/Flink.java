package io.iteratorx.parallels;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.IteratorInputFormat;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.AutoCloseableIterator;
import io.iteratorx.Consumer;

public class Flink implements Parallels {

	public static class IteratorInputFormatProxy extends IteratorInputFormat<JSONObject> {
		private static final long serialVersionUID = 6007638560942548638L;

		private static final Map<UUID, Iterator<JSONObject>> iterators = new HashMap<>();

		public static IteratorInputFormatProxy create(final Iterator<JSONObject> iterator) {
			final UUID uuid = UUID.randomUUID();
			iterators.put(uuid, iterator);
			return new IteratorInputFormatProxy(new AutoCloseableIterator<JSONObject>() {
				private static final long serialVersionUID = 4641584376245923845L;

				@Override
				public boolean hasNext() {
					return iterators.get(uuid).hasNext();
				}

				@Override
				public JSONObject next() {
					return iterators.get(uuid).next();
				}

				@Override
				public void close() throws Exception {
					final Iterator<JSONObject> iterator = iterators.get(uuid);
					if (iterator instanceof AutoCloseable) {
						((AutoCloseable) iterator).close();
					}
				}
			});
		}

		private IteratorInputFormatProxy(final Iterator<JSONObject> iterator) {
			super(iterator);
		}

	}

	public static <T> long forEach(final DataSet<T> dataSet, final Consumer<T> onNext) throws Exception {
		return dataSet.filter(t -> {
			onNext.accept(t);
			return true;
		}).count();
	}

	public static Flink from(final Iterator<JSONObject> iterator) throws Exception {
		return new Flink(iterator);
	}

	public static Flink from(final Iterator<JSONObject> iterator, final int parallelism) throws Exception {
		return new Flink(iterator, DefaultParallelism);
	}

	public static Flink from(final Iterable<JSONObject> iterable) throws Exception {
		return new Flink(iterable);
	}

	public static Flink from(final Iterable<JSONObject> iterable, final int parallelism) throws Exception {
		return new Flink(iterable, DefaultParallelism);
	}

	private final DataSet<JSONObject> dataSet;

	public Flink(final Iterator<JSONObject> iterator) {
		this(iterator, DefaultParallelism);
	}

	public Flink(final Iterator<JSONObject> iterator, final int parallelism) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		this.dataSet = env.createInput(IteratorInputFormatProxy.create(iterator));
	}

	public Flink(final Iterable<JSONObject> iterable) {
		this(iterable.iterator());
	}

	public Flink(final Iterable<JSONObject> iterable, final int parallelism) {
		this(iterable.iterator(), parallelism);
	}

	public DataSet<JSONObject> dataSet() {
		return this.dataSet;
	}

	public long forEach(final Consumer<JSONObject> onNext) throws Exception {
		return forEach(this.dataSet, onNext);
	}

	public long forBatch(final Consumer<List<JSONObject>> onNext) throws Exception {
		return forBatch(DefaultBatchSize, onNext);
	}

	public long forBatch(final int batchSize, final Consumer<List<JSONObject>> onNext) throws Exception {
		final DataSet<List<JSONObject>> groupDataSet = dataSet
				.reduceGroup(new GroupReduceFunction<JSONObject, List<JSONObject>>() {
					private static final long serialVersionUID = -1748951891205342549L;

					private final AtomicInteger count = new AtomicInteger();
					private final List<JSONObject> group = new ArrayList<>();

					@Override
					public void reduce(final Iterable<JSONObject> values, final Collector<List<JSONObject>> out)
							throws Exception {
						for (final JSONObject one : values) {
							group.add(one);
							count.getAndIncrement();

							if (group.size() >= batchSize) {
								out.collect(new ArrayList<>(group));
								group.clear();
							}
						}

						if (group.size() > 0) {
							out.collect(new ArrayList<>(group));
							group.clear();
						}
					}

				});

		return forEach(groupDataSet, onNext);
	}
}
